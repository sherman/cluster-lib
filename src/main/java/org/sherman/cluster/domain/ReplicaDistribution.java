package org.sherman.cluster.domain;

import com.google.common.base.Objects;
import java.util.List;

public class ReplicaDistribution {
    private final List<Integer> shards;
    private final int replicas;
    private final List<Integer> nodes;

    public ReplicaDistribution(List<Integer> shards, int replicas, List<Integer> nodes) {
        this.shards = shards;
        this.replicas = replicas;
        this.nodes = nodes;
    }

    public List<Integer> getShards() {
        return shards;
    }

    public int getReplicas() {
        return replicas;
    }

    public List<Integer> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicaDistribution that = (ReplicaDistribution) o;
        return replicas == that.replicas && Objects.equal(shards, that.shards) &&
            Objects.equal(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(shards, replicas, nodes);
    }
}
