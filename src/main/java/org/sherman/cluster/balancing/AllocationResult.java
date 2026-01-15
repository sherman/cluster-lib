package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.Set;
import org.sherman.cluster.domain.SearchShard;

record AllocationResult(BalancingState state, Set<SearchShard> unassignedShards) {
    AllocationResult {
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(unassignedShards, "unassignedShards");
        unassignedShards = Set.copyOf(unassignedShards);
    }
}
