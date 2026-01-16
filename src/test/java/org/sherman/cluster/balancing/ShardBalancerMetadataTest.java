package org.sherman.cluster.balancing;

import java.util.Map;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerMetadataTest {
    /**
     * Verifies allocation prefers the node with lower ingest/disk deltas when shard counts match.
     */
    @Test
    public void allocatesShardToLowerMetadataWeightNode() {
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");
        var hotShard = new SearchShard("hot-index", 1);
        var coolShard = new SearchShard("cool-index", 1);
        var newShard = new SearchShard("fresh-index", 1);

        var metadata = Map.of(
            hotShard, new ShardMetadata(900.0d, 70.0d, true),
            coolShard, new ShardMetadata(10.0d, 5.0d, true),
            newShard, new ShardMetadata(15.0d, 3.0d, true)
        );

        var state = BalancingState.builder()
            .metadataProvider(provider(metadata))
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, hotShard)
            .addAssignedShard(nodeB, coolShard)
            .build();

        var result = new ShardBalancer().allocate(
            state,
            java.util.List.of(newShard),
            java.util.List.of(),
            factors(0.0d, 0.0d, 1.0d, 1.0d)
        );

        Assert.assertTrue(result.state().getAssignedShards(nodeB).contains(newShard));
        Assert.assertFalse(result.unassignedShards().contains(newShard));
    }

    /**
     * Verifies node load sums baseline and shard metadata while counting throttled shards.
     */
    @Test
    public void computesNodeLoadFromBaselineAndMetadata() {
        var node = new SearchNode("node-a");
        var primaryShard = new SearchShard("index", 1);
        var throttledReplica = new SearchShard("index", 2);

        var metadata = Map.of(
            primaryShard, new ShardMetadata(30.0d, 7.0d, true),
            throttledReplica, new ShardMetadata(20.0d, 9.0d, false)
        );

        var state = BalancingState.builder()
            .metadataProvider(provider(metadata))
            .addNode(node, NodeLoad.of(5.0d, 50.0d))
            .addAssignedShard(node, primaryShard)
            .addThrottledShard(node, throttledReplica)
            .build();

        Assert.assertEquals(state.getIngestLoad(node), 12.0d, 1e-6);
        Assert.assertEquals(state.getDiskUsage(node), 100.0d, 1e-6);
    }

    /**
     * Verifies rebalancing reacts to updated shard metadata by selecting a new target node.
     */
    @Test
    public void rebalanceRespondsToMetadataChanges() {
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");
        var hotShard = new SearchShard("hot-index", 2);
        var warmShard = new SearchShard("hot-index", 1);
        var coldShard = new SearchShard("cold-index", 1);

        var initialMetadata = Map.of(
            hotShard, new ShardMetadata(50.0d, 50.0d, true),
            warmShard, new ShardMetadata(50.0d, 50.0d, true),
            coldShard, new ShardMetadata(50.0d, 50.0d, true)
        );

        var initialState = BalancingState.builder()
            .metadataProvider(provider(initialMetadata))
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, hotShard)
            .addAssignedShard(nodeA, warmShard)
            .addAssignedShard(nodeB, coldShard)
            .build();

        var initialResult = new ShardBalancer().rebalance(
            initialState,
            java.util.List.of(),
            factors(0.0d, 0.0d, 1.0d, 1.0d),
            60.0d
        );

        Assert.assertTrue(initialResult.relocations().isEmpty());

        var updatedMetadata = Map.of(
            hotShard, new ShardMetadata(60.0d, 60.0d, true),
            warmShard, new ShardMetadata(100.0d, 100.0d, true),
            coldShard, new ShardMetadata(50.0d, 50.0d, true)
        );

        var updatedState = BalancingState.builder()
            .metadataProvider(provider(updatedMetadata))
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, hotShard)
            .addAssignedShard(nodeA, warmShard)
            .addAssignedShard(nodeB, coldShard)
            .build();

        var rebalanceResult = new ShardBalancer().rebalance(
            updatedState,
            java.util.List.of(),
            factors(0.0d, 0.0d, 1.0d, 1.0d),
            60.0d
        );

        Assert.assertEquals(rebalanceResult.relocations().size(), 1);
        Assert.assertEquals(rebalanceResult.relocations().getFirst().from(), nodeA);
        Assert.assertEquals(rebalanceResult.relocations().getFirst().to(), nodeB);
        Assert.assertEquals(rebalanceResult.relocations().getFirst().shard(), hotShard);
    }

    /**
     * Verifies decider rejections driven by metadata are counted in rebalance metrics.
     */
    @Test
    public void rebalanceReportsMetadataDrivenDeciderRejects() {
        var nodeA = new SearchNode("node-a");
        var nodeB = new SearchNode("node-b");
        var shardOne = new SearchShard("index", 1);
        var shardTwo = new SearchShard("index", 2);
        var shardThree = new SearchShard("index", 3);

        var metadata = Map.of(
            shardOne, new ShardMetadata(10.0d, 2.0d, true),
            shardTwo, new ShardMetadata(10.0d, 2.0d, true),
            shardThree, new ShardMetadata(200.0d, 2.0d, true)
        );

        var state = BalancingState.builder()
            .metadataProvider(provider(metadata))
            .addNode(nodeA, NodeLoad.of(0.0d, 0.0d))
            .addNode(nodeB, NodeLoad.of(0.0d, 0.0d))
            .addAssignedShard(nodeA, shardOne)
            .addAssignedShard(nodeA, shardTwo)
            .addAssignedShard(nodeB, shardThree)
            .build();

        AllocationDecider diskGuard = (shard, node, balancerState) -> {
            if (shard.getId() == 0) {
                return AllocationDecision.YES;
            }
            var diskUsage = balancerState.getDiskUsage(node);
            return diskUsage > 100.0d ? AllocationDecision.NO : AllocationDecision.YES;
        };

        var result = new ShardBalancer().rebalance(
            state,
            java.util.List.of(diskGuard),
            factors(1.0d, 0.0d, 0.0d, 0.0d),
            0.1d
        );

        Assert.assertTrue(result.relocations().isEmpty());
        Assert.assertTrue(result.metrics().deciderRejects() >= 2);
        Assert.assertEquals(result.metrics().relocationAttempts(), 1);
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

    private static ShardMetadataProvider provider(Map<SearchShard, ShardMetadata> metadata) {
        return shard -> metadata.getOrDefault(shard, ShardMetadata.empty());
    }
}
