package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.List;

record RebalanceResult(BalancingState state, List<Relocation> relocations, RebalanceMetrics metrics) {
    RebalanceResult {
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(relocations, "relocations");
        Preconditions.checkNotNull(metrics, "metrics");
        relocations = List.copyOf(relocations);
    }
}
