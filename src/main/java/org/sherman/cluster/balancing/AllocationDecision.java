package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import java.util.Collection;

enum AllocationDecision {
    YES,
    NO,
    THROTTLED;

    static AllocationDecision combine(Collection<AllocationDecision> decisions) {
        Preconditions.checkNotNull(decisions, "decisions");
        var throttled = false;
        for (var decision : decisions) {
            Preconditions.checkNotNull(decision, "decision");
            if (decision == NO) {
                return NO;
            }
            if (decision == THROTTLED) {
                throttled = true;
            }
        }
        return throttled ? THROTTLED : YES;
    }
}
