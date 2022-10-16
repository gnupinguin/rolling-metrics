package io.github.gnupinguin.metrics;

import io.github.gnupinguin.metrics.core.AbstractBucketedFloatingWindowAggregator;
import io.github.gnupinguin.metrics.core.Bucket;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class BucketedFloatingWindowAggregator<Event, Aggregation> extends AbstractBucketedFloatingWindowAggregator<Event, Aggregation> {

    public BucketedFloatingWindowAggregator(int bucketsCount,
                                            int bucketSizeInSeconds,
                                            Supplier<Aggregation> aggregationFactory,
                                            Function<Event, Aggregation> eventAggregator,
                                            BiFunction<Aggregation, Aggregation, Aggregation> aggregationReducer) {
        super(bucketsCount, bucketSizeInSeconds, aggregationFactory, eventAggregator, aggregationReducer);
    }

    @Override
    protected Aggregation aggregateWindow(long currentSecond, Stream<Bucket<Aggregation>> aggregationStream) {
        return aggregationStream.map(Bucket::content)
                .reduce(aggregationFactory.get(), aggregationReducer::apply);
    }

}
