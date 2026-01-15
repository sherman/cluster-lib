package org.sherman.cluster.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.SearchClusterStatistics;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.util.NodeWeight;

public class SearchNodeRepositoryImpl implements SearchNodeRepository {
    private final Map<String, SearchNode> nodes = new HashMap<>();
    private final SearchClusterStatistics searchClusterStatistics = new SearchClusterStatistics();
    private final NodeWeight nodeWeightFunction;

    public SearchNodeRepositoryImpl(NodeWeight nodeWeightFunction) {
        this.nodeWeightFunction = nodeWeightFunction;
    }

    @Override
    public void addNode(SearchNode searchNode) {
        nodes.putIfAbsent(searchNode.getId(), searchNode);
        searchClusterStatistics.addNode(searchNode);
    }

    @Override
    public List<SearchNode> getNodes() {
        return List.copyOf(nodes.values());
    }

    public float getNodeWeight(String indexName, SearchNode searchNode) {
        return nodeWeightFunction.getWeight(
            searchClusterStatistics.getNodeStatistics(searchNode),
            searchClusterStatistics,
            searchNode,
            indexName
        );
    }
}
