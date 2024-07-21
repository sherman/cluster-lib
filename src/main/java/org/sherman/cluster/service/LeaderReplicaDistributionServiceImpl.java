package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.Jobs;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.RoleAwareJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderReplicaDistributionServiceImpl implements LeaderReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(LeaderReplicaDistributionServiceImpl.class);

    @Override
    public Map<Integer, List<RoleAwareJob>> distribute(LeaderReplicaDistribution parameters) {
        var totalCpusAvailable = parameters.totalCpus();
        var totalCpusRequired = parameters.getJobs().stream().mapToInt(Job::getCpus).sum() * parameters.getReplicas();
        if (totalCpusAvailable < totalCpusRequired) {
            logger.error("Not enough cpus, required: [{}], available: [{}]", totalCpusRequired, totalCpusAvailable);
        }

        var nodesToJobs = new HashMap<Integer, Jobs>();
        for (var node : parameters.getNodes()) {
            nodesToJobs.put(node, new Jobs(0, new LinkedHashSet<>()));
        }

        var jobsToLeaders = new HashMap<Job, Boolean>();
        for (var job : parameters.getJobs()) {
            jobsToLeaders.put(job, false);
        }

        var total = 0;
        // distribute all jobs between nodes evenly (based on requirement)
        var maxNumberOfReplicas = parameters.getJobs().size() * parameters.getReplicas();

        while (total < maxNumberOfReplicas) {
            for (var job : parameters.getJobs()) {
                // sort nodes by used cpus
                var sortedNodes = nodesToJobs.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(ImmutableList.toImmutableList());

                // get appropriate node
                for (var nodeWithJobs : sortedNodes) {
                    if (!nodeWithJobs.getValue().contains(job)) {
                        if (!jobsToLeaders.get(job)) {
                            nodeWithJobs.getValue().addLeader(job);
                            jobsToLeaders.put(job, true);
                        } else {
                            nodeWithJobs.getValue().addStandby(job);
                        }
                        break;
                    }
                }

                total++;
            }
        }

        return nodesToJobs.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    e -> ImmutableList.copyOf(e.getValue().getJobs()))
            );
    }
}
