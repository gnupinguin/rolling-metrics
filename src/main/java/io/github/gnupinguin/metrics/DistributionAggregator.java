package io.github.gnupinguin.metrics;

import io.github.gnupinguin.metrics.core.Bucket;
import org.HdrHistogram.Histogram;

import java.util.function.Function;
import java.util.stream.Stream;

public class DistributionAggregator<Event> extends BucketedFloatingWindowAggregator<Event, CachedHistogram> {

    public DistributionAggregator(int bucketsCount, int bucketSizeInSeconds,
                                  Function<Event, Histogram> eventAggregator) {
        super(bucketsCount, bucketSizeInSeconds,
                CachedHistogram::new,
                event -> new CachedHistogram(eventAggregator.apply(event)),
                CachedHistogram::leftSum);
    }

    @Override
    protected CachedHistogram aggregateWindow(long currentSecond, Stream<Bucket<CachedHistogram>> aggregationStream) {
        CachedHistogram aggregate = super.aggregateWindow(currentSecond, aggregationStream);
        return aggregate.cache();
    }

}


