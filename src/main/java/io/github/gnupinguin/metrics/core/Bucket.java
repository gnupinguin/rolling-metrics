package io.github.gnupinguin.metrics.core;


public record Bucket<Output>(long epochSecond, Output content) { }
