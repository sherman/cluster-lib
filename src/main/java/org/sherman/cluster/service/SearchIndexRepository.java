package org.sherman.cluster.service;

import java.util.List;
import org.sherman.cluster.domain.SearchShard;

public interface SearchIndexRepository {
    void addIndexShard(SearchShard shard);

    List<SearchShard> getShardsByIndex(String index);
}
