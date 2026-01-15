package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;

final class WeightingFactors {
    private final double shardFactor;
    private final double indexFactor;
    private final double ingestFactor;
    private final double diskUsageFactor;
    private final double shardTheta;
    private final double indexTheta;
    private final double ingestTheta;
    private final double diskUsageTheta;

    private WeightingFactors(Builder builder) {
        shardFactor = builder.shardFactor;
        indexFactor = builder.indexFactor;
        ingestFactor = builder.ingestFactor;
        diskUsageFactor = builder.diskUsageFactor;
        var sum = shardFactor + indexFactor + ingestFactor + diskUsageFactor;
        Preconditions.checkArgument(sum > 0.0d, "At least one factor must be positive");
        shardTheta = shardFactor / sum;
        indexTheta = indexFactor / sum;
        ingestTheta = ingestFactor / sum;
        diskUsageTheta = diskUsageFactor / sum;
    }

    static Builder builder() {
        return new Builder();
    }

    static WeightingFactors balancedDefaults() {
        return builder()
            .shardFactor(1.0d)
            .indexFactor(1.0d)
            .ingestFactor(1.0d)
            .diskUsageFactor(1.0d)
            .build();
    }

    double shardTheta() {
        return shardTheta;
    }

    double indexTheta() {
        return indexTheta;
    }

    double ingestTheta() {
        return ingestTheta;
    }

    double diskUsageTheta() {
        return diskUsageTheta;
    }

    static final class Builder {
        private double shardFactor;
        private double indexFactor;
        private double ingestFactor;
        private double diskUsageFactor;

        Builder shardFactor(double value) {
            Preconditions.checkArgument(Double.isFinite(value), "shardFactor must be finite");
            Preconditions.checkArgument(value >= 0.0d, "shardFactor must be non-negative");
            shardFactor = value;
            return this;
        }

        Builder indexFactor(double value) {
            Preconditions.checkArgument(Double.isFinite(value), "indexFactor must be finite");
            Preconditions.checkArgument(value >= 0.0d, "indexFactor must be non-negative");
            indexFactor = value;
            return this;
        }

        Builder ingestFactor(double value) {
            Preconditions.checkArgument(Double.isFinite(value), "ingestFactor must be finite");
            Preconditions.checkArgument(value >= 0.0d, "ingestFactor must be non-negative");
            ingestFactor = value;
            return this;
        }

        Builder diskUsageFactor(double value) {
            Preconditions.checkArgument(Double.isFinite(value), "diskUsageFactor must be finite");
            Preconditions.checkArgument(value >= 0.0d, "diskUsageFactor must be non-negative");
            diskUsageFactor = value;
            return this;
        }

        WeightingFactors build() {
            return new WeightingFactors(this);
        }
    }
}
