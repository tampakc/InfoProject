/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package infoproject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.annotations.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.activemq.AMQSource;
import org.apache.flink.streaming.connectors.activemq.AMQSourceConfig;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.w3c.dom.TypeInfo;
//import org.apache.flink.streaming.api.scala.DataStream;

import javax.jms.*;
import javax.xml.crypto.Data;
import java.time.Instant;
import java.util.Date;
import java.text.SimpleDateFormat;

public class ActiveMQRead {

	static final Time windowTime = Time.minutes(2);


	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.getConfig().disableGenericTypes();



		AMQSource<Tuple2<Long, Integer>> amqSource;
		ActiveMQConnectionFactory connectionFactory;
		Session session;
		Connection connection;
		Destination destination;
		MessageConsumer consumer;
		BytesMessage message;
		SimpleStringSchema deserializationSchema = new SimpleStringSchema();;

		ActiveMQConnectionFactory sourceConnectionFactory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_URL);
		connection = sourceConnectionFactory.createConnection();
		connection.start();
		AMQSourceConfig<Tuple2<Long, Integer>> sourceConfig = new AMQSourceConfig.AMQSourceConfigBuilder<Tuple2<Long, Integer>>()
				.setConnectionFactory(sourceConnectionFactory)
				.setDestinationName("Random_numbers")
				.setDeserializationSchema(new TimeSeriesDeserializer())
				.build();

		amqSource = new AMQSource<>(sourceConfig);

		DataStream<Tuple2<Long, Integer>> input = env
				.addSource(amqSource)
				.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<Long, Integer>>() {


					@Override
					public WatermarkGenerator<Tuple2<Long, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
						return new AscendingTimestampsWatermarks<>();
					}

					@Override
					public TimestampAssigner<Tuple2<Long, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
						return new TimestampAssigner<Tuple2<Long, Integer>>() {
							@Override
							public long extractTimestamp(Tuple2<Long, Integer> data, long l) {
								return data.f0;
							}
						};
					}
				})
				.name("transactions");

		final OutputTag<Tuple2<Long, Integer>> lateOutputTag = new OutputTag<Tuple2<Long, Integer>>("late-data"){};

		SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Date>> windowed = input.
				windowAll(TumblingEventTimeWindows.of(windowTime)).
				sideOutputLateData(lateOutputTag).
				apply(new AllWindowFunction<Tuple2<Long, Integer>, Tuple5<Integer, Integer, Integer, Integer, Date>, TimeWindow>() {
						  @Override
						  public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long, Integer>> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Date>> collector) throws Exception {
							  for (Tuple2<Long, Integer> in : iterable)
								  collector.collect(new Tuple5<>(in.f1, in.f1, in.f1, 1, new Date(timeWindow.getStart())));
						  }
					  }

				);

		SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Double, Date>> alerts = windowed.
				keyBy(4).
				reduce(new ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Date>>() {
					@Override
					public Tuple5<Integer, Integer, Integer, Integer, Date> reduce(Tuple5<Integer, Integer, Integer, Integer, Date> t1, Tuple5<Integer, Integer, Integer, Integer, Date> t2) throws Exception {
						return new Tuple5<>(t1.f0 + t2.f0, Math.min(t1.f1, t2.f1), Math.max(t1.f2, t2.f2), t1.f3 + t2.f3, t1.f4);
					}
				}).
				map(new MapFunction<Tuple5<Integer, Integer, Integer, Integer, Date>, Tuple5<Integer, Integer, Integer, Double, Date>>() {
					@Override
					public Tuple5<Integer, Integer, Integer, Double, Date> map(Tuple5<Integer, Integer, Integer, Integer, Date> in) throws Exception {
						return new Tuple5<>(in.f0, in.f1, in.f2, in.f0*1.0/in.f3, in.f4);
					}
				});


		/*SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Double, Date> alerts = input
				//.keyBy(Tuple2<Long, Integer>::getKey)
				.windowAll(TumblingEventTimeWindows.of(windowTime))
				//.sideOutputLateData(lateOutputTag)
				.aggregate(new WindowAggregation())
				.map(new MapFunction<Tuple5<Integer, Integer, Integer, Double, Date>, DailyData>() {
					@Override
					public DailyData map(Tuple5<Integer, Integer, Integer, Double, Date> rawData) throws Exception {
						return new DailyData(rawData.f4, rawData.f0, rawData.f1, rawData.f2, rawData.f3);
					}
				});*/
				/*.windowAll(TumblingEventTimeWindows.of(windowTime))
				.reduce(new ReduceFunction<Tuple5<Long, Long, Long, Long, Long>>() {
					@Override
					public Tuple5<Long, Long, Long, Long, Long> reduce(Tuple5<Long, Long, Long, Long, Long> t0, Tuple5<Long, Long, Long, Long, Long> t1) throws Exception {
						return new Tuple5<>(
								t0.f0 + t1.f0,
								Math.min(t0.f1, t1.f1),
								Math.max(t0.f2, t1.f2),
								t0.f3 + t1.f3,
								t0.f4
						);
					}
				})
				.process(new ProcessFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Long, Long, Long, Double, Long>>() {
					@Override
					public void processElement(Tuple5<Long, Long, Long, Long, Long> longLongLongLongLongTuple5, ProcessFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Long, Long, Long, Double, Long>>.Context context, Collector<Tuple5<Long, Long, Long, Double, Long>> collector) throws Exception {
						
					}
				})*/
				//.name("aggregation");

		input.addSink(new PrintSinkFunction<>());

		CassandraSink.addSink(alerts)
				.setQuery("INSERT INTO infosystems.daily (sum, min, max, avg, window) VALUES (?, ?, ?, ?, ?)")
				.setHost("localhost")
				.build();

		DataStream<Tuple2<Date, Integer>> late = windowed
				.getSideOutput(lateOutputTag)
				.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Date, Integer>>() {
					@Override
					public Tuple2<Date, Integer> map(Tuple2<Long, Integer> in) throws Exception {
						return new Tuple2<>(new Date(in.f0), in.f1);
					}
				});

		late.addSink(new PrintSinkFunction<>());

		CassandraSink.addSink(late)
				.setQuery("INSERT INTO infosystems.late (time, value) VALUES (?, ?);")
				.setHost("localhost")
				.build();


		/*DataStream<Tuple5<LocalDate, Integer, Integer, Integer, Double>> test = input.map(new MapFunction<Tuple2<Long, Integer>, Tuple5<LocalDate, Integer, Integer, Integer, Double>>() {
			@Override
			public Tuple5<LocalDate, Integer, Integer, Integer, Double> map(Tuple2<Long, Integer> tup) throws Exception {
				String pattern = "yyyy-MM-dd";
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
				return new Tuple5<>(LocalDate.fromMillisSinceEpoch(tup.f0),1,1,1,0.0);
			}
		});*/

		/*CassandraSink.addSink(test)
				.setQuery("INSERT INTO infosystems.late (time, value) VALUES (?, ?);")
				.setHost("localhost")
				.build();*/


		/*CassandraSink.addSink(test)
				.setQuery("INSERT INTO infosystems.daily (day, min, max, sum, avg) VALUES (?, ?, ?, ?, ?);")
				.setHost("localhost")
				.build();

		 */

		//DataStream<Tuple1<LocalDate>> test2 = env.fromElements(new Tuple1<>(LocalDate.fromMillisSinceEpoch(1645217449)));
		DataStream<Tuple1<Date>> test2 = env.fromElements(new Tuple1<>(new Date()));
		//DataStream<Tuple1<String>> test2 = env.fromElements(new Tuple1<>("2011-02-03 04:05+0000"), new Tuple1<>("2012-02-03 04:05+0000"));
 		CassandraSink.addSink(test2).
				setQuery("INSERT INTO infosystems.dates (id) VALUES(?);") .
				setHost("localhost").
				build();

		env.execute("ActiveMQ Aggregate");
	}
}
