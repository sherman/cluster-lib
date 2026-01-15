package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;

record RebalanceMetrics(int relocationAttempts, int relocationsCompleted, int deciderRejects, int throttledShardsCounted) {
    RebalanceMetrics {
        Preconditions.checkArgument(relocationAttempts >= 0, "relocationAttempts must be non-negative");
        Preconditions.checkArgument(relocationsCompleted >= 0, "relocationsCompleted must be non-negative");
        Preconditions.checkArgument(deciderRejects >= 0, "deciderRejects must be non-negative");
        Preconditions.checkArgument(throttledShardsCounted >= 0, "throttledShardsCounted must be non-negative");
    }

    static Builder builder() {
        return new Builder();
    }

    static final class Builder {
        private int relocationAttempts;
        private int relocationsCompleted;
        private int deciderRejects;
        private int throttledShardsCounted;

        Builder addRelocationAttempt() {
            relocationAttempts++;
            return this;
        }

        Builder addRelocationCompleted() {
            relocationsCompleted++;
            return this;
        }

        Builder addDeciderReject() {
            deciderRejects++;
            return this;
        }

        Builder throttledShards(int count) {
            Preconditions.checkArgument(count >= 0, "count must be non-negative");
            throttledShardsCounted = count;
            return this;
        }

        RebalanceMetrics build() {
            return new RebalanceMetrics(relocationAttempts, relocationsCompleted, deciderRejects, throttledShardsCounted);
        }
    }
}
