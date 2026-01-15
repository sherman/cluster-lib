package org.sherman.cluster.service;

import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;

public interface SearchNodeBalancerService {
    SearchNode assignShard(SearchShard shard);
}
