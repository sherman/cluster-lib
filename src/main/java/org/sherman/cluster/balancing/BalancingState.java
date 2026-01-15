package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

final class BalancingState {
    private final Map<SearchNode, NodeState> nodes;

    private BalancingState(Map<SearchNode, NodeState> nodes) {
        this.nodes = nodes;
    }

    static Builder builder() {
        return new Builder();
    }

    List<SearchNode> getNodes() {
        return List.copyOf(nodes.keySet());
    }

    int getShardCount(SearchNode node) {
        return getNodeState(node).getShardCount();
    }

    int getShardCount(SearchNode node, String index) {
        Preconditions.checkNotNull(index, "index");
        return getNodeState(node).getShardCount(index);
    }

    int getIndexCount(SearchNode node) {
        return getNodeState(node).getIndexCount();
    }

    double getIngestLoad(SearchNode node) {
        return getNodeState(node).getLoad().getIngestLoad();
    }

    double getDiskUsage(SearchNode node) {
        return getNodeState(node).getLoad().getDiskUsage();
    }

    Set<SearchShard> getAssignedShards(SearchNode node) {
        return getNodeState(node).getAssignedShards();
    }

    Set<SearchShard> getThrottledShards(SearchNode node) {
        return getNodeState(node).getThrottledShards();
    }

    Set<SearchShard> getAllShards(SearchNode node) {
        return getNodeState(node).getAllShards();
    }

    List<String> getIndices() {
        var indices = new HashSet<String>();
        for (var nodeState : nodes.values()) {
            indices.addAll(nodeState.getIndices());
        }
        var ordered = new ArrayList<>(indices);
        Collections.sort(ordered);
        return List.copyOf(ordered);
    }

    OptionalInt getNextShardId(SearchNode node, String index, int shardId) {
        Preconditions.checkNotNull(index, "index");
        return getNodeState(node).getNextShardId(index, shardId);
    }

    List<SearchShard> getAssignedShardsByIndexDesc(SearchNode node, String index) {
        Preconditions.checkNotNull(index, "index");
        return getNodeState(node).getAssignedShardsByIndexDesc(index);
    }

    boolean containsShard(SearchNode node, SearchShard shard) {
        Preconditions.checkNotNull(shard, "shard");
        return getNodeState(node).containsShard(shard);
    }

    BalancingState copy() {
        var copyNodes = new LinkedHashMap<SearchNode, NodeState>();
        for (var entry : nodes.entrySet()) {
            copyNodes.put(entry.getKey(), entry.getValue().copy());
        }
        return new BalancingState(copyNodes);
    }

    void addShard(SearchNode node, SearchShard shard, AllocationDecision decision) {
        Preconditions.checkNotNull(shard, "shard");
        Preconditions.checkNotNull(decision, "decision");
        getNodeState(node).addShard(shard, decision);
    }

    void removeAssignedShard(SearchNode node, SearchShard shard) {
        Preconditions.checkNotNull(shard, "shard");
        getNodeState(node).removeAssignedShard(shard);
    }

    private NodeState getNodeState(SearchNode node) {
        Preconditions.checkNotNull(node, "node");
        var nodeState = nodes.get(node);
        Preconditions.checkArgument(nodeState != null, "Node [%s] is not registered", node.getId());
        return nodeState;
    }

    static final class Builder {
        private final Map<SearchNode, NodeState> nodes = new LinkedHashMap<>();

        Builder addNode(SearchNode node, NodeLoad load) {
            Preconditions.checkNotNull(node, "node");
            Preconditions.checkNotNull(load, "load");
            Preconditions.checkArgument(!nodes.containsKey(node), "Node [%s] is already registered", node.getId());
            nodes.put(node, new NodeState(node, load));
            return this;
        }

        Builder addAssignedShard(SearchNode node, SearchShard shard) {
            Preconditions.checkNotNull(shard, "shard");
            getNodeState(node).addShard(shard, AllocationDecision.YES);
            return this;
        }

        Builder addThrottledShard(SearchNode node, SearchShard shard) {
            Preconditions.checkNotNull(shard, "shard");
            getNodeState(node).addShard(shard, AllocationDecision.THROTTLED);
            return this;
        }

        BalancingState build() {
            var copyNodes = new LinkedHashMap<SearchNode, NodeState>();
            for (var entry : nodes.entrySet()) {
                copyNodes.put(entry.getKey(), entry.getValue().copy());
            }
            return new BalancingState(copyNodes);
        }

        private NodeState getNodeState(SearchNode node) {
            Preconditions.checkNotNull(node, "node");
            var nodeState = nodes.get(node);
            Preconditions.checkArgument(nodeState != null, "Node [%s] is not registered", node.getId());
            return nodeState;
        }
    }

    private static final class NodeState {
        private final SearchNode node;
        private final NodeLoad load;
        private final Map<SearchShard, AllocationDecision> shardDecisions = new HashMap<>();
        private final Map<String, NavigableSet<Integer>> shardIdsByIndex = new HashMap<>();

        private NodeState(SearchNode node, NodeLoad load) {
            this.node = node;
            this.load = load;
        }

        NodeLoad getLoad() {
            return load;
        }

        void addShard(SearchShard shard, AllocationDecision decision) {
            Preconditions.checkNotNull(shard.getIndex(), "shard index");
            Preconditions.checkNotNull(decision, "decision");
            Preconditions.checkArgument(!shardDecisions.containsKey(shard), "Shard [%s] already exists on node [%s]", shard, node.getId());
            shardDecisions.put(shard, decision);
            var ids = shardIdsByIndex.computeIfAbsent(shard.getIndex(), ignored -> new TreeSet<>());
            ids.add(shard.getId());
        }

        void removeAssignedShard(SearchShard shard) {
            var decision = shardDecisions.get(shard);
            Preconditions.checkArgument(decision != null, "Shard [%s] is not allocated on node [%s]", shard, node.getId());
            Preconditions.checkArgument(decision == AllocationDecision.YES, "Shard [%s] is not assigned on node [%s]", shard, node.getId());
            shardDecisions.remove(shard);
            var ids = shardIdsByIndex.get(shard.getIndex());
            if (ids != null) {
                ids.remove(shard.getId());
                if (ids.isEmpty()) {
                    shardIdsByIndex.remove(shard.getIndex());
                }
            }
        }

        boolean containsShard(SearchShard shard) {
            return shardDecisions.containsKey(shard);
        }

        int getShardCount() {
            return shardDecisions.size();
        }

        int getShardCount(String index) {
            var ids = shardIdsByIndex.get(index);
            return ids == null ? 0 : ids.size();
        }

        int getIndexCount() {
            return shardIdsByIndex.size();
        }

        List<String> getIndices() {
            return List.copyOf(shardIdsByIndex.keySet());
        }

        OptionalInt getNextShardId(String index, int shardId) {
            var ids = shardIdsByIndex.get(index);
            if (ids == null) {
                return OptionalInt.empty();
            }
            var higher = ids.higher(shardId);
            return higher == null ? OptionalInt.empty() : OptionalInt.of(higher);
        }

        Set<SearchShard> getAssignedShards() {
            return filterShardsByDecision(AllocationDecision.YES);
        }

        Set<SearchShard> getThrottledShards() {
            return filterShardsByDecision(AllocationDecision.THROTTLED);
        }

        Set<SearchShard> getAllShards() {
            return Set.copyOf(shardDecisions.keySet());
        }

        List<SearchShard> getAssignedShardsByIndexDesc(String index) {
            var shards = new ArrayList<SearchShard>();
            for (var entry : shardDecisions.entrySet()) {
                var shard = entry.getKey();
                if (entry.getValue() == AllocationDecision.YES && shard.getIndex().equals(index)) {
                    shards.add(shard);
                }
            }
            shards.sort((left, right) -> Integer.compare(right.getId(), left.getId()));
            return List.copyOf(shards);
        }

        NodeState copy() {
            var copy = new NodeState(node, load);
            for (var entry : shardDecisions.entrySet()) {
                copy.addShard(entry.getKey(), entry.getValue());
            }
            return copy;
        }

        private Set<SearchShard> filterShardsByDecision(AllocationDecision decision) {
            var shards = new HashSet<SearchShard>();
            for (var entry : shardDecisions.entrySet()) {
                if (entry.getValue() == decision) {
                    shards.add(entry.getKey());
                }
            }
            return Set.copyOf(shards);
        }
    }
}
