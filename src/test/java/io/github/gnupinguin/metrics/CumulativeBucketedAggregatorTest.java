package io.github.gnupinguin.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;


class CumulativeBucketedAggregatorTest {

    private CumulativeBucketedAggregator<Integer, Long> adder;

    @BeforeEach
    void setUp() {
        adder = new CumulativeBucketedAggregator<>(3, 1, () -> 0L, a -> (long) a, Long::sum);
    }

    @Test
    void testSingleThreadAdder() throws Exception {
        for (int i = 0; i < 10; i++) {
            adder.consumeEvent(1);
        }
        TimeUnit.SECONDS.sleep(1);
        assertEquals(10, adder.measure());
    }

    @Test
    void testBucketSize() throws Exception {
        adder = new CumulativeBucketedAggregator<>(2, 2, () -> 0L, a -> (long) a, Long::sum);
        adder.consumeEvent(1);
        TimeUnit.SECONDS.sleep(2);
        assertEquals(1, adder.measure());

        adder.consumeEvent(1);
        TimeUnit.SECONDS.sleep(2);

        assertEquals(2, adder.measure());
    }

    @Test
    void testMultiThreadsAdder() throws Exception {
        var threadPool = Executors.newFixedThreadPool(4);
        var tasks = IntStream.range(0, 4)
                .mapToObj(i -> threadPool.submit(() -> {
                    for (int j = i * 100 + 1; j <= (i + 1) * 100; j++) {
                        adder.consumeEvent(1);
                    }
                })).toList();

        while (tasks.stream().noneMatch(Future::isDone)) {
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.SECONDS.sleep(1);
        assertEquals(400, adder.measure());
    }

    @Test
    void testOutdatedBucketsAreMeteredInMeasurement() throws Exception {
        adder = new CumulativeBucketedAggregator<>(1, 1, () -> 0L, a -> (long) a, Long::sum);
        adder.consumeEvent(1);
        TimeUnit.SECONDS.sleep(1);
        adder.consumeEvent(1);
        TimeUnit.SECONDS.sleep(1);

        assertEquals(2, adder.measure());
    }

}