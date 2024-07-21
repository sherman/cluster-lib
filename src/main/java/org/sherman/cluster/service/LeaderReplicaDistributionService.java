package org.sherman.cluster.service;

import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.ReplicaDistribution;

public interface LeaderReplicaDistributionService {

    Map<Integer, List<Job>> distribute(LeaderReplicaDistribution parameters);
}
