package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.Jobs;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderReplicaDistributionServiceImplV2 implements LeaderReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(LeaderReplicaDistributionServiceImplV2.class);

    @Override
    public Map<Integer, List<Job>> distribute(LeaderReplicaDistribution parameters) {
        var totalCpusAvailable = parameters.totalCpus();
        var totalCpusRequired = parameters.getJobs().stream().mapToInt(Job::getCpus).sum() * parameters.getReplicas();
        if (totalCpusAvailable < totalCpusRequired) {
            logger.error("Not enough cpus, required: [{}], available: [{}]", totalCpusRequired, totalCpusAvailable);
        }

        // firstly, sort all jobs by cpu in the descending order
        var jobs = parameters.getJobs().stream()
            .sorted(Comparator.comparing(Job::getCpus).reversed())
            .collect(ImmutableList.toImmutableList());

        // track used cpu per node
        var cpuPerNodes = new HashMap<Integer, Integer>();
        for (var node : parameters.getNodes()) {
            cpuPerNodes.put(node, 0);
        }

        var nodesToJobs = new HashMap<Integer, Jobs>();
        for (var node : parameters.getNodes()) {
            nodesToJobs.put(node, new Jobs(0, new LinkedHashSet<>()));
        }

        // distribute all leader jobs between nodes evenly (based on requirement)
        for (var job : jobs) {
            var nodesSortedByCpu = cpuPerNodes.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .collect(ImmutableList.toImmutableList());

            for (var node : nodesSortedByCpu) {
                nodesToJobs.get(node).addJob(job);
                var current = cpuPerNodes.get(node);
                cpuPerNodes.put(node, current + job.getCpus());
                break;
            }
        }

        // distribute all replicas jobs between nodes evenly (based on requirement)
        for (var job : jobs) {
            var nodesSortedByCpu = cpuPerNodes.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .collect(ImmutableList.toImmutableList());

            for (var node : nodesSortedByCpu) {
                if (!nodesToJobs.get(node).getJobs().contains(job)) {
                    nodesToJobs.get(node).addJob(job);
                    var current = cpuPerNodes.get(node);
                    cpuPerNodes.put(node, current + job.getCpus());
                    break;
                }
            }
        }

        logger.info("Total cpu per node: [{}]", cpuPerNodes);

        return nodesToJobs.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    e -> ImmutableList.copyOf(e.getValue().getJobs()))
            );
    }
}
