package io.github.gnupinguin.metrics;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class BucketedFloatingWindowAggregatorTest {

    private BucketedFloatingWindowAggregator<Integer, Integer> aggregator =
            new BucketedFloatingWindowAggregator<>(3, 1, () -> 0, a -> a, Integer::sum);

    @Test
    void testOnlyActualBucketsAreUsedForMeasurement() throws Exception {
        for (int i = 0; i < 4; i++) {
            aggregator.consumeEvent(1);
            TimeUnit.SECONDS.sleep(1);
        }
        assertEquals(3, aggregator.measure());
    }

}