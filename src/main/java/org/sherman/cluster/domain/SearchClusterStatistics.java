package org.sherman.cluster.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class SearchClusterStatistics {
    private final Map<SearchNode, SearchNodeStatistics> perNodeStatistics = new HashMap<>();

    public void addNode(SearchNode searchNode) {
        perNodeStatistics.computeIfAbsent(searchNode, ignored -> new SearchNodeStatistics());
    }

    @Nullable
    public SearchNodeStatistics getNodeStatistics(SearchNode searchNode) {
        return perNodeStatistics.get(searchNode);
    }

    public List<String> getIndices() {
        return perNodeStatistics.values().stream()
            .flatMap(s -> s.getIndices().stream())
            .toList();
    }

    public float getAverageShardsPerNode() {
        var shards = perNodeStatistics.values().stream()
            .mapToInt(SearchNodeStatistics::getShards)
            .sum();

        return ((float) shards) / perNodeStatistics.size();
    }

    public float getAverageShardsByIndex(String index) {
        if (perNodeStatistics.isEmpty()) {
            return 0.0f;
        }

        var indexShards = perNodeStatistics.values().stream()
            .mapToInt(stat -> stat.getShards(index))
            .sum();

        return ((float) indexShards) / perNodeStatistics.size();
    }
}
