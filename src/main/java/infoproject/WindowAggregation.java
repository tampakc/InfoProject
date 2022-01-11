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
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.sql.Time;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static java.util.Calendar.*;

/**
 * Skeleton code for implementing a fraud detector.
 */

// Accumulator holds (SUM, MIN, MAX, COUNT, WINDOW) in a Tuple4, and aggregate function returns (SUM, MIN, MAX, AVG, WINDOW)
public class WindowAggregation
		implements AggregateFunction<Tuple2<Long, Integer>, Tuple5<Integer, Integer, Integer, Integer, Long>, Tuple5<Integer, Integer, Integer, Double, LocalDate>> {
	@Override
	public Tuple5<Integer, Integer, Integer, Integer, Long> createAccumulator() {
		long time = new Date().getTime();
		return new Tuple5<>(0, Integer.MAX_VALUE, Integer.MIN_VALUE, 0, time);
	}

	@Override
	public Tuple5<Integer, Integer, Integer, Integer, Long> add(Tuple2<Long, Integer> value, Tuple5<Integer, Integer, Integer, Integer, Long> accumulator) {
		return new Tuple5<Integer, Integer, Integer, Integer, Long>(
				accumulator.f0 + value.f1,
				Math.min(accumulator.f1, value.f1),
				Math.max(accumulator.f2, value.f1),
				accumulator.f3 + 1,
				new Date(value.f0).getTime()
		);
	}

	@Override
	public Tuple5<Integer, Integer, Integer, Double, LocalDate> getResult(Tuple5<Integer, Integer, Integer, Integer, Long> accumulator) {
		LocalDate day = LocalDate.fromMillisSinceEpoch(accumulator.f4);
		return new Tuple5<>(
				accumulator.f0,
				accumulator.f1,
				accumulator.f2,
				((double) accumulator.f0)/accumulator.f3,
				day
		);
	}

	@Override
	public Tuple5<Integer, Integer, Integer, Integer, Long> merge(Tuple5<Integer, Integer, Integer, Integer, Long> a, Tuple5<Integer, Integer, Integer, Integer, Long> b) {
		return new Tuple5<>(a.f0 + b.f0, Math.min(a.f1, b.f1), Math.max(a.f2, b.f2), a.f3 + b.f3, a.f4);
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