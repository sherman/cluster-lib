package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

/**
 * Denies allocation when the target node already contains the same shard group (index + id).
 */
final class ReplicaSeparationDecider implements AllocationDecider {
    @Override
    public AllocationDecision canAllocate(SearchShard shard, SearchNode node, BalancingState state) {
        Preconditions.checkNotNull(shard, "shard");
        Preconditions.checkNotNull(node, "node");
        Preconditions.checkNotNull(state, "state");
        return state.containsShard(node, shard) ? AllocationDecision.NO : AllocationDecision.YES;
    }
}
