package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

final class ShardBalancer {
    private static final double WEIGHT_EPSILON = 1e-6d;
    private static final int MAX_RELOCATIONS = 10_000;

    AllocationResult allocate(
        BalancingState state,
        List<SearchShard> shards,
        List<AllocationDecider> deciders,
        WeightingFactors factors
    ) {
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(shards, "shards");
        Preconditions.checkNotNull(deciders, "deciders");
        Preconditions.checkNotNull(factors, "factors");
        var workingState = state.copy();
        var remaining = new LinkedHashSet<SearchShard>();
        for (var shard : shards) {
            Preconditions.checkNotNull(shard, "shard");
            remaining.add(shard);
        }

        if (workingState.getNodes().isEmpty()) {
            return new AllocationResult(workingState, remaining);
        }

        for (var shard : shards) {
            // Filter nodes by deciders; THROTTLED still counts as a valid target per spec.
            var candidateDecisions = new LinkedHashMap<SearchNode, AllocationDecision>();
            for (var node : workingState.getNodes()) {
                var decision = evaluateDeciders(deciders, shard, node, workingState);
                if (decision != AllocationDecision.NO) {
                    candidateDecisions.put(node, decision);
                }
            }
            if (candidateDecisions.isEmpty()) {
                continue;
            }

            // Compute weights once per shard placement to keep decisions consistent.
            var weights = computeWeights(workingState, factors);
            SearchNode selectedNode = null;
            AllocationDecision selectedDecision = null;
            double selectedWeight = 0.0d;

            for (var entry : candidateDecisions.entrySet()) {
                var node = entry.getKey();
                var weight = weights.get(node);
                if (selectedNode == null) {
                    selectedNode = node;
                    selectedDecision = entry.getValue();
                    selectedWeight = weight;
                    continue;
                }

                // Prefer the smallest weight; apply a deterministic tie-breaker when equal.
                if (weight < selectedWeight - WEIGHT_EPSILON) {
                    selectedNode = node;
                    selectedDecision = entry.getValue();
                    selectedWeight = weight;
                    continue;
                }

                if (Math.abs(weight - selectedWeight) <= WEIGHT_EPSILON) {
                    var preferred = pickTieBreaker(selectedNode, node, workingState, shard);
                    if (!preferred.equals(selectedNode)) {
                        selectedNode = node;
                        selectedDecision = entry.getValue();
                        selectedWeight = weight;
                    }
                }
            }

            // THROTTLED assignments are kept in state but remain unassigned.
            workingState.addShard(selectedNode, shard, selectedDecision);
            if (selectedDecision == AllocationDecision.YES) {
                remaining.remove(shard);
            }
        }

        return new AllocationResult(workingState, remaining);
    }

    RebalanceResult rebalance(
        BalancingState state,
        List<AllocationDecider> deciders,
        WeightingFactors factors,
        double threshold
    ) {
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(deciders, "deciders");
        Preconditions.checkNotNull(factors, "factors");
        Preconditions.checkArgument(Double.isFinite(threshold), "threshold must be finite");
        Preconditions.checkArgument(threshold >= 0.0d, "threshold must be non-negative");

        var workingState = state.copy();
        var relocations = new ArrayList<Relocation>();
        var metrics = RebalanceMetrics.builder();
        metrics.throttledShards(countThrottledShards(workingState));
        var indices = sortIndicesByImbalance(workingState);
        for (var index : indices) {
            var relevantNodes = findRelevantNodes(workingState, deciders, index);
            if (relevantNodes.size() < 2) {
                continue;
            }
            // Keep the node list sorted by weight and move shards from heaviest to lightest.
            var nodesByWeight = sortNodesByWeight(workingState, relevantNodes, factors);
            if (nodesByWeight.getLast().weight() - nodesByWeight.getFirst().weight() < threshold) {
                continue;
            }
            var lowIndex = 0;
            var highIndex = nodesByWeight.size() - 1;

            while (lowIndex < highIndex) {
                if (relocations.size() >= MAX_RELOCATIONS) {
                    return new RebalanceResult(workingState, relocations, metrics.build());
                }

                var low = nodesByWeight.get(lowIndex);
                var high = nodesByWeight.get(highIndex);
                // Stop when the max-min delta is below the threshold.
                if (high.weight() - low.weight() < threshold) {
                    break;
                }

                // Try to relocate the highest-id shard first from heavy to light node.
                var relocation = tryRelocate(index, high.node(), low.node(), workingState, deciders, metrics);
                metrics.addRelocationAttempt();
                if (relocation.isPresent()) {
                    relocations.add(relocation.get());
                    metrics.addRelocationCompleted();
                    nodesByWeight = sortNodesByWeight(workingState, relevantNodes, factors);
                    lowIndex = 0;
                    highIndex = nodesByWeight.size() - 1;
                    continue;
                }

                // Rotate the low/high pointers as described in the spec.
                if (lowIndex < highIndex - 1) {
                    lowIndex++;
                } else if (lowIndex > 0) {
                    lowIndex = 0;
                    highIndex--;
                } else {
                    break;
                }
            }
        }

        return new RebalanceResult(workingState, relocations, metrics.build());
    }

    private AllocationDecision evaluateDeciders(
        List<AllocationDecider> deciders,
        SearchShard shard,
        SearchNode node,
        BalancingState state
    ) {
        if (deciders.isEmpty()) {
            return AllocationDecision.YES;
        }
        var decisions = new ArrayList<AllocationDecision>(deciders.size());
        for (var decider : deciders) {
            Preconditions.checkNotNull(decider, "decider");
            var decision = decider.canAllocate(shard, node, state);
            Preconditions.checkNotNull(decision, "decision");
            decisions.add(decision);
        }
        return AllocationDecision.combine(decisions);
    }

    private Map<SearchNode, Double> computeWeights(BalancingState state, WeightingFactors factors) {
        var nodes = state.getNodes();
        Preconditions.checkArgument(!nodes.isEmpty(), "Cluster must have at least one node");

        double totalShards = 0.0d;
        double totalIndices = 0.0d;
        double totalIngest = 0.0d;
        double totalDisk = 0.0d;
        for (var node : nodes) {
            totalShards += state.getShardCount(node);
            totalIndices += state.getIndexCount(node);
            totalIngest += state.getIngestLoad(node);
            totalDisk += state.getDiskUsage(node);
        }

        var count = nodes.size();
        var avgShards = totalShards / count;
        var avgIndices = totalIndices / count;
        var avgIngest = totalIngest / count;
        var avgDisk = totalDisk / count;

        var weights = new HashMap<SearchNode, Double>();
        for (var node : nodes) {
            var shardWeight = state.getShardCount(node) - avgShards;
            var indexWeight = state.getIndexCount(node) - avgIndices;
            var ingestWeight = state.getIngestLoad(node) - avgIngest;
            var diskWeight = state.getDiskUsage(node) - avgDisk;
            var weight = factors.shardTheta() * shardWeight
                + factors.indexTheta() * indexWeight
                + factors.ingestTheta() * ingestWeight
                + factors.diskUsageTheta() * diskWeight;
            weights.put(node, weight);
        }
        return weights;
    }

    private SearchNode pickTieBreaker(
        SearchNode current,
        SearchNode candidate,
        BalancingState state,
        SearchShard shard
    ) {
        // Prefer the node with the closest higher shard id for the same index.
        var currentNext = state.getNextShardId(current, shard.getIndex(), shard.getId());
        var candidateNext = state.getNextShardId(candidate, shard.getIndex(), shard.getId());
        if (candidateNext.isPresent() && currentNext.isEmpty()) {
            return candidate;
        }
        if (candidateNext.isEmpty() && currentNext.isPresent()) {
            return current;
        }
        if (candidateNext.isPresent()) {
            var cmp = Integer.compare(candidateNext.getAsInt(), currentNext.getAsInt());
            if (cmp < 0) {
                return candidate;
            }
            if (cmp > 0) {
                return current;
            }
        }
        return candidate.getId().compareTo(current.getId()) < 0 ? candidate : current;
    }

    private List<String> sortIndicesByImbalance(BalancingState state) {
        var indices = new ArrayList<>(state.getIndices());
        var imbalance = new HashMap<String, Integer>();
        for (var index : indices) {
            // Imbalance is computed as max-min shard count across nodes for this index.
            var min = Integer.MAX_VALUE;
            var max = Integer.MIN_VALUE;
            for (var node : state.getNodes()) {
                var count = state.getShardCount(node, index);
                min = Math.min(min, count);
                max = Math.max(max, count);
            }
            imbalance.put(index, max == Integer.MIN_VALUE ? 0 : max - min);
        }

        indices.sort((left, right) -> {
            var cmp = Integer.compare(imbalance.get(right), imbalance.get(left));
            return cmp != 0 ? cmp : left.compareTo(right);
        });
        return List.copyOf(indices);
    }

    private List<SearchNode> findRelevantNodes(
        BalancingState state,
        List<AllocationDecider> deciders,
        String index
    ) {
        var relevant = new ArrayList<SearchNode>();
        var probeShard = new SearchShard(index, 0);
        for (var node : state.getNodes()) {
            var decision = evaluateDeciders(deciders, probeShard, node, state);
            if (decision != AllocationDecision.NO) {
                relevant.add(node);
            }
        }
        return List.copyOf(relevant);
    }

    private List<WeightedNode> sortNodesByWeight(
        BalancingState state,
        List<SearchNode> nodes,
        WeightingFactors factors
    ) {
        var weights = computeWeights(state, factors);
        var weighted = new ArrayList<WeightedNode>(nodes.size());
        for (var node : nodes) {
            weighted.add(new WeightedNode(node, weights.get(node)));
        }
        weighted.sort(Comparator.comparingDouble(WeightedNode::weight).thenComparing(left -> left.node().getId()));
        return List.copyOf(weighted);
    }

    private Optional<Relocation> tryRelocate(
        String index,
        SearchNode from,
        SearchNode to,
        BalancingState state,
        List<AllocationDecider> deciders,
        RebalanceMetrics.Builder metrics
    ) {
        if (from.equals(to)) {
            return Optional.empty();
        }
        // Move the highest shard id first, skipping shards already on the target node.
        var candidates = state.getAssignedShardsByIndexDesc(from, index);
        for (var shard : candidates) {
            if (state.containsShard(to, shard)) {
                continue;
            }
            var decision = evaluateDeciders(deciders, shard, to, state);
            if (decision == AllocationDecision.YES) {
                state.removeAssignedShard(from, shard);
                state.addShard(to, shard, AllocationDecision.YES);
                return Optional.of(new Relocation(from, to, shard));
            }
            if (decision == AllocationDecision.NO) {
                metrics.addDeciderReject();
            }
        }
        return Optional.empty();
    }

    private int countThrottledShards(BalancingState state) {
        var total = 0;
        for (var node : state.getNodes()) {
            total += state.getThrottledShards(node).size();
        }
        return total;
    }

    private record WeightedNode(SearchNode node, double weight) {
        private WeightedNode {
            Preconditions.checkNotNull(node, "node");
        }
    }
}
