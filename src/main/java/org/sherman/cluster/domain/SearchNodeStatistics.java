package org.sherman.cluster.domain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SearchNodeStatistics {
    private final Map<String, Set<SearchShard>> shards = new HashMap<>();

    public void addShard(SearchShard shard) {
        var indexShards = shards.computeIfAbsent(shard.getIndex(), ignored -> new HashSet<>());
        indexShards.add(shard);
    }

    public int getShards() {
        return shards.values().stream().mapToInt(Set::size).sum();
    }

    public int getShards(String index) {
        var indexShards = shards.get(index);
        return indexShards == null ? 0 : indexShards.size();
    }

    public List<String> getIndices() {
        return List.copyOf(shards.keySet());
    }
}
