package org.sherman.cluster.util;

import com.google.common.base.Preconditions;
import org.sherman.cluster.domain.SearchClusterStatistics;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchNodeStatistics;

public class NodeWeight {
    private final float theta0;
    private final float theta1;

    public NodeWeight(float shardFactor, float indexFactor) {
        var sum = shardFactor + indexFactor;
        Preconditions.checkArgument(sum > 0.0f, "Sum of factors must be positive!");
        theta0 = shardFactor / sum;
        theta1 = indexFactor / sum;
    }

    public float getWeight(
        SearchNodeStatistics nodeStatistics,
        SearchClusterStatistics clusterStatistics,
        SearchNode searchNode,
        String index
    ) {
        var shardWeight = nodeStatistics.getShards() - clusterStatistics.getAverageShardsPerNode();
        var indexWeight = nodeStatistics.getShards(index) - clusterStatistics.getAverageShardsByIndex(index);
        return theta0 * shardWeight + theta1 * indexWeight;
    }
}
