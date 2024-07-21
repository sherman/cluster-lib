package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.List;

public class LeaderReplicaDistribution {
    private final List<Job> jobs;
    private final int replicas;
    private final List<Integer> nodes;

    private final int cpusPerNode;

    public LeaderReplicaDistribution(List<Job> jobs, int replicas, List<Integer> nodes, int cpusPerNode) {
        this.jobs = jobs;
        this.replicas = replicas;
        this.nodes = nodes;
        this.cpusPerNode = cpusPerNode;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public int getReplicas() {
        return replicas;
    }

    public List<Integer> getNodes() {
        return nodes;
    }

    public int totalCpus() {
        return nodes.size() * cpusPerNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeaderReplicaDistribution that = (LeaderReplicaDistribution) o;
        return replicas == that.replicas && cpusPerNode == that.cpusPerNode && Objects.equal(jobs, that.jobs) &&
            Objects.equal(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(jobs, replicas, nodes, cpusPerNode);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("jobs", jobs)
            .add("replicas", replicas)
            .add("nodes", nodes)
            .add("cpusPerNode", cpusPerNode)
            .toString();
    }
}
