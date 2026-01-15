package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;

/**
 * Captures per-node load inputs for weighting. Values should be normalized (e.g. 0..1 fractions or
 * percentages) so ingest and disk usage remain comparable across nodes.
 */
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
