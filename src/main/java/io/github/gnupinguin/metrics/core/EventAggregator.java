package io.github.gnupinguin.metrics.core;

public interface EventAggregator<Event, Aggregation> {

    void consumeEvent(Event event);

    Aggregation measure();

}
