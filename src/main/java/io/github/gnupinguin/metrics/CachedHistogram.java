package io.github.gnupinguin.metrics;

import org.HdrHistogram.Histogram;

public record CachedHistogram(Histogram histogram, double mean, double percentile95, double percentile99) {

    public CachedHistogram(Histogram histogram){
        this(histogram, .0, .0, .0);
    }

    public CachedHistogram() {
        this(new Histogram(3));
    }

    public CachedHistogram cache() {
        return new CachedHistogram(histogram, histogram.getMean(), histogram.getValueAtPercentile(.95), histogram.getValueAtPercentile(.99));
    }

    CachedHistogram leftSum(CachedHistogram histogram) {
        this.histogram.add(histogram.histogram);
        return this;
    }

    public static Histogram fromNumber(long n) {
        var h = new Histogram(3);
        h.recordValue(n);
        return h;
    }

}
