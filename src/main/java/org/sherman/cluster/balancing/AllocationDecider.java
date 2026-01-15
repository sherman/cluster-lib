package org.sherman.cluster.balancing;

import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

@FunctionalInterface
interface AllocationDecider {
    AllocationDecision canAllocate(SearchShard shard, SearchNode node, BalancingState state);
}
