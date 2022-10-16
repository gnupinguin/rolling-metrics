package io.github.gnupinguin.metrics.core;

import java.time.Clock;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// idea: opened bucket (last bucket) does not participate at aggregation
public abstract class AbstractBucketedFloatingWindowAggregator<Event, Aggregation> implements EventAggregator<Event, Aggregation> {

    protected final int bucketsCount;

    protected final int bucketSizeInSeconds;

    protected final Supplier<Aggregation> aggregationFactory;

    protected final BiFunction<Aggregation, Aggregation, Aggregation> aggregationReducer;

    protected final Function<Event, Aggregation> eventAggregator;

    private final Clock clock = Clock.systemUTC();

    private final List<Bucket<Aggregation>> window;

    private Bucket<Aggregation> lastWindowAggregation;

    private final long startSecond;

    public AbstractBucketedFloatingWindowAggregator(int bucketsCount,
                                                    int bucketSizeInSeconds,
                                                    Supplier<Aggregation> aggregationFactory,
                                                    Function<Event, Aggregation> eventAggregator,
                                                    BiFunction<Aggregation, Aggregation, Aggregation> aggregationReducer) {
        this.bucketsCount = bucketsCount;
        this.bucketSizeInSeconds = bucketSizeInSeconds;
        this.aggregationFactory = aggregationFactory;
        this.aggregationReducer = aggregationReducer;
        this.eventAggregator = eventAggregator;

        startSecond = epochSecond();

        window = IntStream.range(0, bucketsCount)
                .mapToObj(i -> new Bucket<>(startSecond, aggregationFactory.get()))
                .collect(Collectors.toList());

        lastWindowAggregation = new Bucket<>(startSecond, aggregationFactory.get());
    }

    @Override
    public final synchronized void consumeEvent(Event event) {
        long currentSecond = epochSecond();
        moveWindow(currentSecond, aggregation -> aggregationReducer.apply(aggregation, eventAggregator.apply(event)));
    }

    protected abstract Aggregation aggregateWindow(long lastBucket, Stream<Bucket<Aggregation>> aggregationStream);

    @Override
    public final synchronized Aggregation measure() {
        moveWindow(epochSecond(), Function.identity());
        return lastWindowAggregation.content();
    }

    private void moveWindow(long epochSecond, Function<Aggregation, Aggregation> updateBucket) {
        long timeRange = epochSecond - startSecond;
        int i = (int) ((timeRange / bucketSizeInSeconds) % bucketsCount);
        var bucket = window.get(i);
        long effectiveSecond = timeRange - timeRange % bucketSizeInSeconds;
        if (isOutdatedBucket(effectiveSecond, bucket)) {
            var aggregation = aggregateWindow(effectiveSecond, actualWindow());
            lastWindowAggregation = new Bucket<>(effectiveSecond, aggregation);
            window.set(i, new Bucket<>(effectiveSecond, updateBucket.apply(aggregationFactory.get())));
        } else {
            window.set(i, new Bucket<>(effectiveSecond, updateBucket.apply(bucket.content())));
        }
    }

    private boolean isOutdatedBucket(long effectiveSecond, Bucket<Aggregation> bucket) {
        return bucket.epochSecond() != effectiveSecond;
    }

    private Stream<Bucket<Aggregation>> actualWindow() {
        var range = (long) bucketSizeInSeconds * bucketsCount;
        return window.stream()
                .filter(b -> b.epochSecond() + range > lastWindowAggregation.epochSecond());
    }

    protected long epochSecond() {
        return clock.instant().getEpochSecond();
    }

}
