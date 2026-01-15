package org.sherman.cluster.balancing;

import com.google.common.base.Preconditions;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

record Relocation(SearchNode from, SearchNode to, SearchShard shard) {
    Relocation {
        Preconditions.checkNotNull(from, "from");
        Preconditions.checkNotNull(to, "to");
        Preconditions.checkNotNull(shard, "shard");
    }
}
