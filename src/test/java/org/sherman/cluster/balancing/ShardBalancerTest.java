package org.sherman.cluster.balancing;

import java.util.List;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerTest {
    /**
     * Verifies that allocation selects the lowest-weight node for a new shard.
     */
    @Test
    public void assignsShardToLowestWeightNode() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("nodeA");
        var nodeB = new SearchNode("nodeB");
        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeB, new SearchShard("index", 1))
            .addAssignedShard(nodeB, new SearchShard("index", 2))
            .build();

        var shard = new SearchShard("index", 3);
        var result = balancer.allocate(
            state,
            List.of(shard),
            List.of(),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        Assert.assertTrue(result.state().getAssignedShards(nodeA).contains(shard));
        Assert.assertFalse(result.unassignedShards().contains(shard));
    }

    /**
     * Verifies that the tie-breaker favors the node with the next higher shard id.
     */
    @Test
    public void tieBreakerPrefersNextHigherShardId() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("nodeA");
        var nodeB = new SearchNode("nodeB");
        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 5))
            .addAssignedShard(nodeB, new SearchShard("index", 7))
            .build();

        var shard = new SearchShard("index", 6);
        var result = balancer.allocate(
            state,
            List.of(shard),
            List.of(),
            factors(1.0d, 1.0d, 0.0d, 0.0d)
        );

        Assert.assertTrue(result.state().getAssignedShards(nodeB).contains(shard));
    }

    /**
     * Verifies that throttled decisions keep shards unassigned but counted on the node.
     */
    @Test
    public void throttledDecisionKeepsShardUnassignedButAccounted() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("nodeA");
        var nodeB = new SearchNode("nodeB");
        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeB, new SearchShard("index", 1))
            .addAssignedShard(nodeB, new SearchShard("index", 2))
            .build();

        AllocationDecider throttlingDecider = (shard, node, ignored) ->
            node.getId().equals("nodeA") ? AllocationDecision.THROTTLED : AllocationDecision.YES;

        var shard = new SearchShard("index", 3);
        var result = balancer.allocate(
            state,
            List.of(shard),
            List.of(throttlingDecider),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        Assert.assertTrue(result.unassignedShards().contains(shard));
        Assert.assertTrue(result.state().getThrottledShards(nodeA).contains(shard));
        Assert.assertFalse(result.state().getAssignedShards(nodeA).contains(shard));
    }

    /**
     * Verifies that rebalancing moves the highest shard id first from a heavy node.
     */
    @Test
    public void rebalancesByMovingHighestIdShardFirst() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("nodeA");
        var nodeB = new SearchNode("nodeB");
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
        var relocation = result.relocations().get(0);
        Assert.assertEquals(relocation.from(), nodeA);
        Assert.assertEquals(relocation.to(), nodeB);
        Assert.assertEquals(relocation.shard(), new SearchShard("index", 3));
    }

    /**
     * Verifies primary/replica copies of the same shard are allocated to different nodes.
     */
    @Test
    public void separatesPrimaryAndReplicaOnAllocation() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("nodeA");
        var nodeB = new SearchNode("nodeB");
        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .build();

        var primary = new SearchShard("index", 1);
        var replica = new SearchShard("index", 1);

        var result = balancer.allocate(
            state,
            List.of(primary, replica),
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        var assignedA = result.state().getAssignedShards(nodeA);
        var assignedB = result.state().getAssignedShards(nodeB);
        Assert.assertEquals(assignedA.size() + assignedB.size(), 2);
        Assert.assertTrue(assignedA.size() <= 1, "Node A should not host both copies");
        Assert.assertTrue(assignedB.size() <= 1, "Node B should not host both copies");
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
