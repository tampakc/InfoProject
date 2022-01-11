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

import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.activemq.AMQSource;
import org.apache.flink.streaming.connectors.activemq.AMQSourceConfig;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.sink.AlertSink;

import javax.jms.*;

public class ActiveMQRead {






	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		AMQSource<TimeSeriesData> amqSource;
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
		AMQSourceConfig<TimeSeriesData> sourceConfig = new AMQSourceConfig.AMQSourceConfigBuilder<TimeSeriesData>()
				.setConnectionFactory(sourceConnectionFactory)
				.setDestinationName("Random_numbers")
				.setDeserializationSchema(new TimeSeriesDeserializer())
				.build();

		amqSource = new AMQSource<>(sourceConfig);

		DataStream<TimeSeriesData> input = env
				.addSource(amqSource)
				.assignTimestampsAndWatermarks(new WatermarkStrategy<TimeSeriesData>() {


					@Override
					public WatermarkGenerator<TimeSeriesData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
						return new AscendingTimestampsWatermarks<>();
					}

					@Override
					public TimestampAssigner<TimeSeriesData> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
						return new TimestampAssigner<TimeSeriesData>() {
							@Override
							public long extractTimestamp(TimeSeriesData timeSeriesData, long l) {
								return timeSeriesData.getTimestamp();
							}
						};
					}
				})
				.name("transactions");

		//input.print();

		DataStream<Tuple4<Long, Long, Long, Double>> alerts = input
				.keyBy(TimeSeriesData::getKey)
				.window(TumblingEventTimeWindows.of(Time.minutes(2)))
				.aggregate(new WindowAggregation())
				.name("aggregation");

		alerts
				.map(new MapFunction<Tuple4<Long, Long, Long, Double>, String>() {
			@Override
			public String map(Tuple4<Long, Long, Long, Double> Tup) throws Exception {
				String str1 = Tup.f0.toString();
				String str2 = Tup.f1.toString();
				String str3 = Tup.f2.toString();
				String str4 = Tup.f3.toString();

				return str1 + " " + str2 + " " + str3 + " " + str4;
			}
		})
				.addSink(new PrintSinkFunction<>());
		//input.addSink(new PrintSinkFunction<TimeSeriesData>());


		/*DataStream<Alert> alerts = transactions
			.keyBy(Transaction::getAccountId)
			.process(new FraudDetector())
			.name("fraud-detector");*/

		/*alerts
			.addSink(new AlertSink())
			.name("send-alerts");*/

		env.execute("ActiveMQ Aggregate");
	}
}
