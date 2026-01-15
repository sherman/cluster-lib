package org.sherman.cluster.balancing;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

@RunWith(JUnitQuickcheck.class)
public class ShardBalancerQuickcheckProperties {
    private static final int MAX_TRIALS = 200;
    private static final double REBALANCE_THRESHOLD = 1.1d;

    @Property(trials = MAX_TRIALS)
    public void allocateAssignsAllShardsWhenNodesAvailable(
        @InRange(min = "0", max = "100") int nodesCount,
        @InRange(min = "0", max = "100") int indexCount,
        @InRange(min = "1", max = "100") int shardCount,
        @InRange(min = "0", max = "100") int ingestSeed,
        @InRange(min = "0", max = "100") int diskSeed
    ) {
        Assume.assumeTrue(nodesCount > 0);
        var effectiveShards = effectiveShardCount(indexCount, shardCount);
        var shards = createShards(indexCount, effectiveShards);

        var builder = BalancingState.builder();
        var nodes = addNodes(builder, nodesCount, ingestSeed, diskSeed);

        var result = new ShardBalancer().allocate(
            builder.build(),
            shards,
            List.of(),
            factors(1.0d, 1.0d, 1.0d, 1.0d)
        );

        Assert.assertTrue(result.unassignedShards().isEmpty());
        var assigned = collectAssigned(result.state(), nodes);
        Assert.assertEquals(assigned.size(), shards.size());
        Assert.assertTrue(assigned.containsAll(shards));
    }

    @Property(trials = MAX_TRIALS)
    public void allocateKeepsShardsUnassignedWhenNoNodes(
        @InRange(min = "0", max = "100") int nodesCount,
        @InRange(min = "0", max = "100") int indexCount,
        @InRange(min = "1", max = "100") int shardCount,
        @InRange(min = "0", max = "100") int ingestSeed,
        @InRange(min = "0", max = "100") int diskSeed
    ) {
        Assume.assumeTrue(nodesCount == 0);
        var effectiveShards = effectiveShardCount(indexCount, shardCount);
        var shards = createShards(indexCount, effectiveShards);

        var result = new ShardBalancer().allocate(
            BalancingState.builder().build(),
            shards,
            List.of(),
            factors(1.0d, 1.0d, 1.0d, 1.0d)
        );

        Assert.assertEquals(result.unassignedShards().size(), shards.size());
        Assert.assertTrue(result.state().getNodes().isEmpty());
    }

    @Property(trials = MAX_TRIALS)
    public void rebalancePreservesShardSet(
        @InRange(min = "0", max = "100") int nodesCount,
        @InRange(min = "0", max = "100") int indexCount,
        @InRange(min = "1", max = "100") int shardCount,
        @InRange(min = "0", max = "100") int ingestSeed,
        @InRange(min = "0", max = "100") int diskSeed
    ) {
        Assume.assumeTrue(nodesCount > 0);
        var effectiveShards = effectiveShardCount(indexCount, shardCount);
        var shards = createShards(indexCount, effectiveShards);

        var builder = BalancingState.builder();
        var nodes = addNodes(builder, nodesCount, ingestSeed, diskSeed);

        if (!shards.isEmpty()) {
            var first = nodes.get(0);
            for (var shard : shards) {
                builder.addAssignedShard(first, shard);
            }
        }

        var result = new ShardBalancer().rebalance(
            builder.build(),
            List.of(),
            factors(1.0d, 1.0d, 1.0d, 1.0d),
            REBALANCE_THRESHOLD
        );

        var assigned = collectAssigned(result.state(), nodes);
        Assert.assertEquals(assigned, new HashSet<>(shards));
    }

    private static int effectiveShardCount(int indexCount, int shardCount) {
        return indexCount == 0 ? 0 : shardCount;
    }

    private static List<SearchNode> addNodes(
        BalancingState.Builder builder,
        int nodesCount,
        int ingestSeed,
        int diskSeed
    ) {
        var nodes = new ArrayList<SearchNode>(nodesCount);
        for (var i = 1; i <= nodesCount; i++) {
            var node = new SearchNode("node-" + i);
            nodes.add(node);
            var ingestLoad = seedLoad(ingestSeed, i, 9);
            var diskUsage = seedLoad(diskSeed, i, 7);
            builder.addNode(node, NodeLoad.of(ingestLoad, diskUsage));
        }
        return nodes;
    }

    private static double seedLoad(int seed, int index, int modulo) {
        if (seed == 0) {
            return 0.0d;
        }
        return (seed * (index % modulo)) / (double) modulo;
    }

    private static List<SearchShard> createShards(int indexCount, int shardCount) {
        if (indexCount <= 0 || shardCount <= 0) {
            return List.of();
        }
        var shards = new ArrayList<SearchShard>(shardCount);
        for (var i = 1; i <= shardCount; i++) {
            var index = "index-" + ((i - 1) % indexCount + 1);
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
