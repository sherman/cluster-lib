package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;

final class NodeLoad {
    private final double ingestLoad;
    private final double diskUsage;

    private NodeLoad(double ingestLoad, double diskUsage) {
        Preconditions.checkArgument(Double.isFinite(ingestLoad), "ingestLoad must be finite");
        Preconditions.checkArgument(Double.isFinite(diskUsage), "diskUsage must be finite");
        this.ingestLoad = ingestLoad;
        this.diskUsage = diskUsage;
    }

    static NodeLoad of(double ingestLoad, double diskUsage) {
        return new NodeLoad(ingestLoad, diskUsage);
    }

    double getIngestLoad() {
        return ingestLoad;
    }

    double getDiskUsage() {
        return diskUsage;
    }
}
