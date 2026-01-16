package org.sherman.cluster.balancing;

import org.jetbrains.annotations.Nullable;
import org.sherman.cluster.domain.SearchShard;

@FunctionalInterface
interface ShardMetadataProvider {
    @Nullable
    ShardMetadata getMetadata(SearchShard shard);

    static ShardMetadataProvider noop() {
        return shard -> ShardMetadata.empty();
    }
}
