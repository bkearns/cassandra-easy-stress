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
package org.apache.cassandra.easystress

import com.codahale.metrics.MetricRegistry
import org.apache.cassandra.easystress.generators.Registry
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.BoundStatement
import org.apache.cassandra.easystress.collector.Collector
import org.apache.cassandra.easystress.commands.Run
import org.apache.cassandra.easystress.workloads.IStressRunner
import org.apache.cassandra.easystress.workloads.Operation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import io.mockk.mockk
import java.util.concurrent.TimeUnit

/**
 * Every operation that COMPLETES must land in the latency timer, including the ones that fail.
 *
 * `OperationCallback.accept` records the latency only on the success path:
 *
 *     // success
 *     context.timer(op, populatePhase).update(endNanos - op.createdAtNanos, NANOSECONDS)
 *     ...
 *     // failure
 *     context.metrics.errors.mark()
 *     context.collect(...)
 *     return
 *
 * A failed operation therefore increments the error meter but never touches the timer, so the
 * percentiles the console prints (and that `RateLimiterOptimizer` acts on) are computed over
 * successes alone. The excluded samples are not a random subset: an operation that fails is
 * normally one that waited longest — a timeout is the tail by definition, and a request dropped
 * because the queue was full never ran at all. Dropping them biases every reported percentile
 * downward and hides exactly the regime an operator is trying to see.
 *
 * The failure path already has the number it needs: `op.createdAtNanos` is captured when the
 * operation is built, so `System.nanoTime() - op.createdAtNanos` is the same queue-inclusive
 * duration the success path records.
 */
class OperationCallbackFailureLatencyTest {
    private val noopCollector =
        object : Collector {
            override fun collect(
                ctx: StressContext,
                op: Operation,
                result: Either<AsyncResultSet, Throwable>,
                startNanos: Long,
                endNanos: Long,
            ) = Unit
        }

    private fun stressContext(metrics: Metrics): StressContext =
        StressContext(
            session = mockk(relaxed = true),
            mainArguments = mockk<Run>(relaxed = true),
            thread = 0,
            metrics = metrics,
            registry = Registry(),
            rateLimiter = null,
            collector = noopCollector,
        )

    /** An operation built `ageMillis` ago, standing in for one that waited in the request queue. */
    private fun mutationAgedBy(ageMillis: Long): Operation.Mutation {
        val op = Operation.Mutation(mockk<BoundStatement>(relaxed = true))
        // `createdAtNanos` is fixed at construction. Wait so the synthetic age is real rather than
        // faked through reflection: the assertion below is about the value the callback computes
        // from it, so it has to be a genuine timestamp.
        Thread.sleep(ageMillis)
        return op
    }

    @Test
    fun `a failed operation is recorded in the latency timer`() {
        val metrics = Metrics(MetricRegistry(), emptyList(), 0)
        val context = stressContext(metrics)
        val op = mutationAgedBy(50)
        val before = metrics.mutations.count

        OperationCallback(context, mockk<IStressRunner>(relaxed = true), op, System.nanoTime(), false)
            .accept(null, RuntimeException("ReadTimeout"))

        assertThat(metrics.mutations.count)
            .describedAs(
                "a failed operation must be counted in the latency distribution; without it the " +
                    "percentiles are computed over successes only and omit the slowest outcomes",
            ).isEqualTo(before + 1)
    }

    @Test
    fun `a failed operation contributes its queue-inclusive duration, not zero`() {
        val metrics = Metrics(MetricRegistry(), emptyList(), 0)
        val context = stressContext(metrics)
        val op = mutationAgedBy(50)

        OperationCallback(context, mockk<IStressRunner>(relaxed = true), op, System.nanoTime(), false)
            .accept(null, RuntimeException("ReadTimeout"))

        // The operation was built at least 50ms ago, so its duration is at least that. A zero or
        // near-zero max means the failure path recorded something it did not measure.
        assertThat(metrics.mutations.snapshot.max)
            .describedAs("the failure path must record the elapsed time since the operation was built")
            .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(50))
    }

    @Test
    fun `a failed operation is still counted as an error`() {
        val metrics = Metrics(MetricRegistry(), emptyList(), 0)
        val context = stressContext(metrics)
        val before = metrics.errors.count

        OperationCallback(context, mockk<IStressRunner>(relaxed = true), mutationAgedBy(0), System.nanoTime(), false)
            .accept(null, RuntimeException("ReadTimeout"))

        assertThat(metrics.errors.count)
            .describedAs("adding the failure to the timer must not stop it being an error")
            .isEqualTo(before + 1)
    }

    /**
     * The converse, so the fix cannot be "record everything twice": a successful operation is
     * recorded exactly once. A failure that is also timed must not make the success path
     * double-count.
     */
    @Test
    fun `a successful operation is recorded exactly once`() {
        val metrics = Metrics(MetricRegistry(), emptyList(), 0)
        val context = stressContext(metrics)
        val before = metrics.mutations.count

        OperationCallback(
            context,
            mockk<IStressRunner>(relaxed = true),
            mutationAgedBy(0),
            System.nanoTime(),
            false,
        ).accept(mockk<AsyncResultSet>(relaxed = true), null)

        assertThat(metrics.mutations.count).isEqualTo(before + 1)
        assertThat(metrics.errors.count).isEqualTo(0)
    }
}
