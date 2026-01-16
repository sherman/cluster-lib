package org.sherman.cluster.balancing;

import java.util.List;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerRebalanceScenarioTest {
    private static final Logger log = LoggerFactory.getLogger(ShardBalancerRebalanceScenarioTest.class);

    /**
     * Verifies shards move to a newly added node during rebalance.
     */
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

    /**
     * Verifies the most imbalanced index is rebalanced first.
     */
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

    /**
     * Verifies rebalance never moves a shard onto a node that already hosts that shard group.
     */
    @Test
    public void rebalanceRespectsReplicaConstraint() {
        var balancer = new ShardBalancer();
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");
        var nodeC = new SearchNode("node-c");

        var shard = new SearchShard("index", 1);

        var state = BalancingState.builder()
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeC, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, shard)
            .addAssignedShard(nodeB, new SearchShard("index", 1))
            .build();

        var result = balancer.rebalance(
            state,
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d),
            0.1d
        );

        Assert.assertTrue(result.relocations().size() >= 1);
        Assert.assertTrue(result.state().getAssignedShards(nodeA).size() <= 1);
        Assert.assertTrue(result.state().getAssignedShards(nodeB).size() <= 1);
    }

    /**
     * Verifies two indices with primaries/replicas balance evenly across four nodes.
     */
    @Test
    public void balancesTwoIndicesEvenly() {
        var balancer = new ShardBalancer();
        var nodes = List.of(
            new SearchNode("node-a"),
            new SearchNode("node-b"),
            new SearchNode("node-c"),
            new SearchNode("node-d")
        );

        var builder = BalancingState.builder();
        for (var node : nodes) {
            builder.addNode(node, NodeLoad.of(0.0d, 0.0d));
        }

        var index1Shards = List.of(
            new SearchShard("index1", 1),
            new SearchShard("index1", 2),
            new SearchShard("index1", 3),
            new SearchShard("index1", 4)
        );
        var index2Shards = List.of(
            new SearchShard("index2", 1),
            new SearchShard("index2", 2),
            new SearchShard("index2", 3),
            new SearchShard("index2", 4)
        );

        var initial = balancer.allocate(
            builder.build(),
            dup(index1Shards),
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        var afterSecondIndex = balancer.allocate(
            initial.state(),
            dup(index2Shards),
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        for (var node : nodes) {
            var count = afterSecondIndex.state().getShardCount(node);
            Assert.assertTrue(count >= 2 && count <= 4, "Unexpected shard count per node: " + count);
            logNodeShards(afterSecondIndex.state(), node);
        }
    }

    /**
     * Verifies two indices with 3 primaries/replicas balance across four nodes.
     */
    @Test
    public void balancesTwoIndicesWithThreeShardsEvenly() {
        var balancer = new ShardBalancer();
        var nodes = List.of(
            new SearchNode("node-a"),
            new SearchNode("node-b"),
            new SearchNode("node-c"),
            new SearchNode("node-d")
        );

        var builder = BalancingState.builder();
        for (var node : nodes) {
            builder.addNode(node, NodeLoad.of(0.0d, 0.0d));
        }

        var index1Shards = List.of(
            new SearchShard("index1", 1),
            new SearchShard("index1", 2),
            new SearchShard("index1", 3)
        );
        var index2Shards = List.of(
            new SearchShard("index2", 1),
            new SearchShard("index2", 2),
            new SearchShard("index2", 3)
        );

        var initial = balancer.allocate(
            builder.build(),
            dup(index1Shards),
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        var afterSecondIndex = balancer.allocate(
            initial.state(),
            dup(index2Shards),
            List.of(new ReplicaSeparationDecider()),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        for (var node : nodes) {
            var count = afterSecondIndex.state().getShardCount(node);
            Assert.assertTrue(count >= 1 && count <= 3, "Unexpected shard count per node: " + count);
            logNodeShards(afterSecondIndex.state(), node);
        }
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

    private static List<SearchShard> dup(List<SearchShard> shards) {
        var copies = new java.util.ArrayList<SearchShard>(shards.size() * 2);
        for (var shard : shards) {
            copies.add(shard);
            copies.add(new SearchShard(shard.getIndex(), shard.getId()));
        }
        return List.copyOf(copies);
    }

    private static void logNodeShards(BalancingState state, SearchNode node) {
        var shards = state.getAssignedShards(node).stream()
            .sorted((l, r) -> {
                var cmp = l.getIndex().compareTo(r.getIndex());
                return cmp != 0 ? cmp : Integer.compare(l.getId(), r.getId());
            })
            .map(shard -> shard.getIndex() + "/" + shard.getId())
            .toList();

        var shardsText = String.join(", ", shards);
        var line = "Node " + node.getId() + " | " + (shardsText.isEmpty() ? "no shards" : shardsText);
        var border = "-".repeat(Math.max(line.length(), 16));
        log.info("{}", border);
        log.info("{}", line);
        log.info("{}", border);
    }
}
