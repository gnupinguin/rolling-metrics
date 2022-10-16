package io.github.gnupinguin.metrics;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DistributionAggregatorTest {

    private final DistributionAggregator<Long> aggregator =
            new DistributionAggregator<>(3, 1, CachedHistogram::fromNumber);

    long[] data =  new long[]{20, 24, 30, 36, 21, 18, 120, 12, 32, 28};

    @Test
    void test() throws Exception {
        Arrays.stream(data).forEach(aggregator::consumeEvent);
        TimeUnit.SECONDS.sleep(1);

        var mean =  (double) Arrays.stream(data).sum() / data.length;
        var measure = aggregator.measure();

        assertEquals(mean, measure.mean());
        assertEquals(12, measure.percentile95());
        assertEquals(12, measure.percentile99());
    }

}