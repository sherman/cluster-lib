package org.sherman.cluster.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.SearchShard;

public class SearchIndexRepositoryImpl implements SearchIndexRepository {
    private final Map<String, List<SearchShard>> indicesToShards = new HashMap<>();

    @Override
    public void addIndexShard(SearchShard shard) {
        var shards = indicesToShards.computeIfAbsent(shard.getIndex(), ignored -> new ArrayList<>());
        shards.add(shard);
    }

    @Override
    public List<SearchShard> getShardsByIndex(String index) {
        var shards = indicesToShards.get(index);
        return shards == null ? List.of() : List.copyOf(shards);
    }
}
