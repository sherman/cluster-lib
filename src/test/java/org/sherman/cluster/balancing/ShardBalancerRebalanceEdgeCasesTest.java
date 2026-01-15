package org.sherman.cluster.balancing;

import java.util.List;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerRebalanceEdgeCasesTest {
    /**
     * Verifies rebalance skips nodes rejected by allocation deciders.
     */
    @Test
    public void rebalanceSkipsNodesRejectedByDecider() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 1))
            .addAssignedShard(nodeA, new SearchShard("index", 2))
            .addAssignedShard(nodeA, new SearchShard("index", 3))
            .build();

        AllocationDecider decider = (shard, node, ignored) ->
            node.equals(nodeB) ? AllocationDecision.NO : AllocationDecision.YES;

        var result = balancer.rebalance(
            state,
            List.of(decider),
            factors(1.0d, 1.0d, 1.0d, 1.0d),
            0.1d
        );

        Assert.assertTrue(result.relocations().isEmpty());
        Assert.assertTrue(result.state().getAssignedShards(nodeB).isEmpty());
    }

    /**
     * Verifies throttled target nodes do not accept relocations.
     */
    @Test
    public void rebalanceHonorsThrottledTarget() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 1))
            .addAssignedShard(nodeA, new SearchShard("index", 2))
            .build();

        AllocationDecider decider = (shard, node, ignored) ->
            node.equals(nodeB) ? AllocationDecision.THROTTLED : AllocationDecision.YES;

        var result = balancer.rebalance(
            state,
            List.of(decider),
            factors(1.0d, 1.0d, 1.0d, 1.0d),
            0.1d
        );

        Assert.assertTrue(result.relocations().isEmpty());
        Assert.assertTrue(result.state().getAssignedShards(nodeB).isEmpty());
    }

    /**
     * Verifies that the most imbalanced index is processed first.
     */
    @Test
    public void rebalanceUsesMostImbalancedIndexFirst() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index-1", 1))
            .addAssignedShard(nodeA, new SearchShard("index-1", 2))
            .addAssignedShard(nodeB, new SearchShard("index-1", 3))
            .addAssignedShard(nodeB, new SearchShard("index-1", 4))
            .addAssignedShard(nodeA, new SearchShard("index-2", 1))
            .addAssignedShard(nodeA, new SearchShard("index-2", 2))
            .addAssignedShard(nodeA, new SearchShard("index-2", 3))
            .build();

        var result = balancer.rebalance(
            state,
            List.of(),
            factors(1.0d, 0.0d, 0.0d, 0.0d),
            1.1d
        );

        Assert.assertFalse(result.relocations().isEmpty());
        Assert.assertEquals(result.relocations().get(0).shard().getIndex(), "index-2");
    }

    /**
     * Verifies rebalance exits when no unique shards can move.
     */
    @Test
    public void rebalanceStopsWhenNoUniqueShardsToMove() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(10.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 1))
            .addAssignedShard(nodeA, new SearchShard("index", 2))
            .addAssignedShard(nodeB, new SearchShard("index", 1))
            .addAssignedShard(nodeB, new SearchShard("index", 2))
            .build();

        var result = balancer.rebalance(
            state,
            List.of(),
            factors(0.0d, 0.0d, 1.0d, 0.0d),
            0.1d
        );

        Assert.assertTrue(result.relocations().isEmpty());
    }

    /**
     * Verifies the threshold stops oscillating moves after a single relocation.
     */
    @Test
    public void rebalanceStopsAfterThresholdPreventsOscillation() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 1))
            .addAssignedShard(nodeA, new SearchShard("index", 2))
            .addAssignedShard(nodeA, new SearchShard("index", 3))
            .build();

        var result = balancer.rebalance(
            state,
            List.of(),
            factors(1.0d, 0.0d, 0.0d, 0.0d),
            1.1d
        );

        Assert.assertEquals(result.relocations().size(), 1);
        Assert.assertEquals(result.state().getShardCount(nodeA), 2);
        Assert.assertEquals(result.state().getShardCount(nodeB), 1);
    }

    private static WeightingFactors factors(
        double shardFactor,
        double indexFactor,
        double ingestFactor,
        double diskUsageFactor
    ) {
        return WeightingFactors.builder()
            .shardFactor(shardFactor)
            .indexFactor(indexFactor)
            .ingestFactor(ingestFactor)
            .diskUsageFactor(diskUsageFactor)
            .build();
    }
}
