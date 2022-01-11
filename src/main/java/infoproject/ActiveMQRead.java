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

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.activemq.AMQSource;
import org.apache.flink.streaming.connectors.activemq.AMQSourceConfig;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.OutputTag;
//import org.apache.flink.streaming.api.scala.DataStream;

import javax.jms.*;
import java.util.Date;

public class ActiveMQRead {

	static final Time windowTime = Time.minutes(2);

	@Table(keyspace = "infosystems", name = "daily")
	public class DailyData {

		@Column(name = "day")
		private LocalDate day = null;

		@Column(name = "sum")
		private int sum = 0;

		@Column(name = "min")
		private int min = Integer.MAX_VALUE;

		@Column(name = "max")
		private int max = Integer.MIN_VALUE;

		@Column(name = "avg")
		private double avg = 0;

		public DailyData() {}

		public DailyData(LocalDate day, int sum, int min, int max, double avg) {
			this.setDay(day);
			this.setSum(sum);
			this.setMin(min);
			this.setMax(max);
			this.setAvg(avg);
		}

		public LocalDate getDay() {
			return day;
		}

		public void setDay(LocalDate day) {
			this.day = day;
		}

		public int getSum() {
			return sum;
		}

		public void setSum(int sum) {
			this.sum = sum;
		}

		public int getMin() {
			return min;
		}

		public void setMin(int min) {
			this.min = min;
		}

		public int getMax() {
			return max;
		}

		public void setMax(int max) {
			this.max = max;
		}

		public double getAvg() {
			return avg;
		}

		public void setAvg(double avg) {
			this.avg = avg;
		}

		@Override
		public String toString() {
			return day.toString() + " : " + getSum() + " " + getMin() + " " + getMax() + " " + getAvg();
		}
	}

	@Table(keyspace = "infosystems", name = "late")
	public class LateEvent {
		@Column(name = "timestamp")
		private Date timestamp = null;

		@Column(name = "value")
		private int value = 0;

		public LateEvent() {}

		public LateEvent(Date timestamp, int value) {
			this.setTimestamp(timestamp);
			this.setValue(value);
		}

		public Date getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Date timestamp) {
			this.timestamp = timestamp;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public String toString() {
			return timestamp.toString() + " : " + value;
		}
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

		SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Double, LocalDate>> alerts = input
				//.keyBy(Tuple2<Long, Integer>::getKey)
				.windowAll(TumblingEventTimeWindows.of(windowTime))
				.sideOutputLateData(lateOutputTag)
				.aggregate(new WindowAggregation())
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
				.name("aggregation");

		alerts
				.map(new MapFunction<Tuple5<Integer, Integer, Integer, Double, LocalDate>, String>() {
			@Override
			public String map(Tuple5<Integer, Integer, Integer, Double, LocalDate> Tup) throws Exception {
				String str1 = Tup.f0.toString();
				String str2 = Tup.f1.toString();
				String str3 = Tup.f2.toString();
				String str4 = Tup.f3.toString();
				String str5 = Tup.f4.toString();

				return str5 + " : " + str1 + " " + str2 + " " + str3 + " " + str4;
			}
		})
				.addSink(new PrintSinkFunction<>());

		CassandraSink.addSink(alerts)
				.setQuery("INSERT INTO infosystems.daily (sum, min, max, avg, day) VALUES (?, ?, ?, ?, ?)")
				.setHost("localhost")
				.build();

		DataStream<Tuple2<Date, Integer>> side = alerts
				.getSideOutput(lateOutputTag)
				.map(new MapFunction<Tuple2<Long, Integer>, Tuple2<Date, Integer>>() {
					@Override
					public Tuple2<Date, Integer> map(Tuple2<Long, Integer> in) throws Exception {
						return new Tuple2<>(new Date(in.f0), in.f1);
					}
				});

		CassandraSink.addSink(side)
				.setQuery("INSERT INTO infosystems.late (time, value) VALUES (?, ?)")
				.setHost("localhost")
				.build();

		env.execute("ActiveMQ Aggregate");
	}
}
