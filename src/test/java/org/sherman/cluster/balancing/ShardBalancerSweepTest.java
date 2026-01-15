package org.sherman.cluster.balancing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ShardBalancerSweepTest {
    private static final double REBALANCE_THRESHOLD = 1.1d;

    @Test(dataProvider = "nodesAndShards")
    public void allocateDistributesAcrossNodes(int nodesCount, int shardsCount) {
        var balancer = new ShardBalancer();
        var nodes = new ArrayList<SearchNode>(nodesCount);
        var builder = BalancingState.builder();
        for (var i = 1; i <= nodesCount; i++) {
            var node = new SearchNode("node-" + i);
            nodes.add(node);
            builder.addNode(node, NodeLoad.of(0.0d, 0.0d));
        }

        var state = builder.build();
        var shards = createShards("index", shardsCount);
        var result = balancer.allocate(
            state,
            shards,
            List.of(),
            factors(1.0d, 0.0d, 0.0d, 0.0d)
        );

        Assert.assertTrue(result.unassignedShards().isEmpty(), "All shards should be assigned");
        var assigned = collectAssigned(result.state(), nodes);
        Assert.assertEquals(assigned.size(), shardsCount, "Every shard must be assigned exactly once");
        for (var shard : shards) {
            Assert.assertTrue(assigned.contains(shard), "Missing shard: " + shard);
        }

        var min = Integer.MAX_VALUE;
        var max = Integer.MIN_VALUE;
        for (var node : nodes) {
            var count = result.state().getShardCount(node);
            min = Math.min(min, count);
            max = Math.max(max, count);
        }
        Assert.assertTrue(max - min <= 1, "Shard distribution is too uneven");
    }

    @Test(dataProvider = "nodesAndShards")
    public void rebalancePreservesShardSet(int nodesCount, int shardsCount) {
        var balancer = new ShardBalancer();
        var nodes = new ArrayList<SearchNode>(nodesCount);
        var builder = BalancingState.builder();
        for (var i = 1; i <= nodesCount; i++) {
            var node = new SearchNode("node-" + i);
            nodes.add(node);
            builder.addNode(node, NodeLoad.of(0.0d, 0.0d));
        }

        var shards = createShards("index", shardsCount);
        var primaryNode = nodes.get(0);
        for (var shard : shards) {
            builder.addAssignedShard(primaryNode, shard);
        }
        var state = builder.build();

        var result = balancer.rebalance(
            state,
            List.of(),
            factors(1.0d, 0.0d, 0.0d, 0.0d),
            REBALANCE_THRESHOLD
        );

        var assigned = collectAssigned(result.state(), nodes);
        Assert.assertEquals(assigned, new HashSet<>(shards), "Rebalance must keep all shards assigned");
        if (nodesCount == 1) {
            Assert.assertTrue(result.relocations().isEmpty(), "Single node should not relocate shards");
        }
    }

    @DataProvider(name = "nodesAndShards")
    public Object[][] nodesAndShards() {
        var data = new Object[100][2];
        var index = 0;
        for (var nodes = 1; nodes <= 10; nodes++) {
            for (var shards = 1; shards <= 10; shards++) {
                data[index][0] = nodes;
                data[index][1] = shards;
                index++;
            }
        }
        return data;
    }

    private static List<SearchShard> createShards(String index, int count) {
        var shards = new ArrayList<SearchShard>(count);
        for (var i = 1; i <= count; i++) {
            shards.add(new SearchShard(index, i));
        }
        return List.copyOf(shards);
    }

    private static Set<SearchShard> collectAssigned(BalancingState state, List<SearchNode> nodes) {
        var assigned = new HashSet<SearchShard>();
        for (var node : nodes) {
            assigned.addAll(state.getAssignedShards(node));
        }
        return assigned;
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
