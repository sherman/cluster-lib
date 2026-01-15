package org.sherman.cluster.balancing;

import java.util.List;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerRebalanceScenarioTest {
    @Test
    public void rebalanceMovesShardsAfterNodeAdded() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");
        var nodeC = new SearchNode("node-c");

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(10.0d, 50.0d))
            .addNode(nodeB, NodeLoad.of(50.0d, 100.0d))
            .addNode(nodeC, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, new SearchShard("index", 1))
            .addAssignedShard(nodeA, new SearchShard("index", 2))
            .addAssignedShard(nodeA, new SearchShard("index", 3))
            .addAssignedShard(nodeA, new SearchShard("index", 4))
            .addAssignedShard(nodeA, new SearchShard("index", 5))
            .addAssignedShard(nodeA, new SearchShard("index", 6))
            .build();

        var result = balancer.rebalance(
            state,
            List.of(),
            factors(1.0d, 1.0d, 1.0d, 1.0d),
            0.1d
        );

        Assert.assertFalse(result.relocations().isEmpty());
        var movedToNewNode = result.relocations().stream()
            .anyMatch(relocation -> relocation.to().equals(nodeC));
        Assert.assertTrue(movedToNewNode, "Expected relocation to the new node");
        Assert.assertFalse(result.state().getAssignedShards(nodeC).isEmpty());
    }

    @Test
    public void rebalanceTargetsNewIndexFirst() {
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
        Assert.assertFalse(result.state().getAssignedShards(nodeB).stream()
            .filter(shard -> shard.getIndex().equals("index-2"))
            .toList()
            .isEmpty());
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
