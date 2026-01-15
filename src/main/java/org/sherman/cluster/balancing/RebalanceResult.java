package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.List;

record RebalanceResult(BalancingState state, List<Relocation> relocations) {
    RebalanceResult {
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(relocations, "relocations");
        relocations = List.copyOf(relocations);
    }
}
