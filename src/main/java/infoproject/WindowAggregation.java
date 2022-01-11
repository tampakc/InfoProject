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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.sql.Time;

/**
 * Skeleton code for implementing a fraud detector.
 */

// Accumulator holds (SUM, MIN, MAX, COUNT) in a Tuple4, and aggregate function returns (SUM, MIN, MAX, AVG)
public class WindowAggregation
		implements AggregateFunction<TimeSeriesData, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Double>> {
	@Override
	public Tuple4<Long, Long, Long, Long> createAccumulator() {
		return new Tuple4<>(0L, Long.MAX_VALUE, Long.MIN_VALUE, 0L);
	}

	@Override
	public Tuple4<Long, Long, Long, Long> add(TimeSeriesData value, Tuple4<Long, Long, Long, Long> accumulator) {
		Long val = (long) value.getValue();
		return new Tuple4<>(
				accumulator.f0 + value.getValue(),
				Math.min(accumulator.f1, val),
				Math.max(accumulator.f2, val),
				accumulator.f3 + 1
		);
	}

	@Override
	public Tuple4<Long, Long, Long, Double> getResult(Tuple4<Long, Long, Long, Long> accumulator) {
		return new Tuple4<>(
				accumulator.f0,
				accumulator.f1,
				accumulator.f2,
				((double) accumulator.f0)/accumulator.f3
		);
	}

	@Override
	public Tuple4<Long, Long, Long, Long> merge(Tuple4<Long, Long, Long, Long> a, Tuple4<Long, Long, Long, Long> b) {
		return new Tuple4<>(a.f0 + b.f0, Math.min(a.f1, b.f1), Math.max(a.f2, b.f2), a.f3 + b.f3);
	}
}

/*public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		Alert alert = new Alert();
		alert.setId(transaction.getAccountId());

		collector.collect(alert);
	}
}
*/