package org.sherman.cluster.service;

import org.sherman.cluster.domain.SearchClusterStatistics;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.sherman.cluster.util.NodeWeight;

public class SearchNodeBalancerServiceImpl implements SearchNodeBalancerService {
    private final SearchNodeRepository searchNodeRepository;

    public SearchNodeBalancerServiceImpl(SearchNodeRepository searchNodeRepository) {
        this.searchNodeRepository = searchNodeRepository;
    }

    @Override
    public SearchNode assignShard(SearchShard shard) {
        var nodes = searchNodeRepository.getNodes();
        return null;
    }
}
