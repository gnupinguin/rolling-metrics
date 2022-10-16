package io.github.gnupinguin.metrics;

import io.github.gnupinguin.metrics.core.Bucket;
import io.github.gnupinguin.metrics.core.AbstractBucketedFloatingWindowAggregator;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class CumulativeBucketedAggregator<Event, Aggregation> extends AbstractBucketedFloatingWindowAggregator<Event, Aggregation> {

    private Bucket<Aggregation> lastAggregation;

    public CumulativeBucketedAggregator(int bucketsCount,
                                        int bucketSizeInSeconds,
                                        Supplier<Aggregation> aggregationFactory,
                                        Function<Event, Aggregation> eventAggregator,
                                        BiFunction<Aggregation, Aggregation, Aggregation> aggregationReducer) {
        super(bucketsCount, bucketSizeInSeconds, aggregationFactory, eventAggregator, aggregationReducer);
        lastAggregation = new Bucket<>(epochSecond(), aggregationFactory.get());
    }

    @Override
    protected Aggregation aggregateWindow(long currentSecond, Stream<Bucket<Aggregation>> aggregationStream) {
        Aggregation aggregation = aggregationStream.filter(b -> b.epochSecond() >= lastAggregation.epochSecond())
                .map(Bucket::content)
                .reduce(aggregationFactory.get(), aggregationReducer::apply);
        aggregation = aggregationReducer.apply(lastAggregation.content(), aggregation);
        lastAggregation = new Bucket<>(currentSecond, aggregation);
        return lastAggregation.content();
    }

}
