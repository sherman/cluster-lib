package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;

record ShardMetadata(double diskUsageDelta, double ingestLoadDelta, boolean primary) {
    ShardMetadata {
        Preconditions.checkArgument(Double.isFinite(diskUsageDelta), "diskUsageDelta must be finite");
        Preconditions.checkArgument(Double.isFinite(ingestLoadDelta), "ingestLoadDelta must be finite");
        Preconditions.checkArgument(diskUsageDelta >= 0.0d, "diskUsageDelta must be non-negative");
        Preconditions.checkArgument(ingestLoadDelta >= 0.0d, "ingestLoadDelta must be non-negative");
    }

    static ShardMetadata empty() {
        return new ShardMetadata(0.0d, 0.0d, false);
    }
}
