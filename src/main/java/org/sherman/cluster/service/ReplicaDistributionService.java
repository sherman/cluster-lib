package org.sherman.cluster.service;

import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.ReplicaDistribution;

public interface ReplicaDistributionService {

    Map<Integer, List<Integer>> distribute(ReplicaDistribution parameters);
}
