package org.sherman.cluster.service;

import java.util.List;
import org.sherman.cluster.domain.SearchNode;

public interface SearchNodeRepository {
    void addNode(SearchNode searchNode);

    List<SearchNode> getNodes();
}
