package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.Jobs;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.Role;
import org.sherman.cluster.domain.RoleAwareJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderReplicaDistributionServiceImplV2 implements LeaderReplicaDistributionServiceV2 {
    private static final Logger logger = LoggerFactory.getLogger(LeaderReplicaDistributionServiceImplV2.class);

    @Override
    public Map<Integer, List<RoleAwareJob>> distribute(
        LeaderReplicaDistribution parameters,
        Map<Integer, List<RoleAwareJob>> previousState
    ) {
        var totalCpusAvailable = parameters.totalCpus();
        var totalCpusRequired = parameters.getJobs().stream().mapToInt(Job::getCpus).sum() * parameters.getReplicas();
        if (totalCpusAvailable < totalCpusRequired) {
            logger.error("Not enough cpus, required: [{}], available: [{}]", totalCpusRequired, totalCpusAvailable);
        }

        // firstly, sort all jobs by cpu in the descending order
        var jobs = parameters.getJobs().stream()
            .sorted(
                Comparator.comparing(Job::getCpus).reversed()
                    .thenComparing(Job::getId)
            )
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
                .sorted(
                    Comparator.comparing((Function<Map.Entry<Integer, Integer>, Integer>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)
                )
                .map(Map.Entry::getKey)
                .collect(ImmutableList.toImmutableList());

            for (var node : nodesSortedByCpu) {
                nodesToJobs.get(node).addLeader(job);
                cpuPerNodes.computeIfPresent(node, (k, current) -> current + job.getCpus());
                break;
            }
        }

        // distribute all replicas jobs between nodes evenly (based on requirement)
        /*for (var job : jobs) {
            var nodesSortedByCpu = cpuPerNodes.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .collect(ImmutableList.toImmutableList());

            for (var node : nodesSortedByCpu) {
                if (!nodesToJobs.get(node).contains(job)) {
                    nodesToJobs.get(node).addStandby(job);
                    cpuPerNodes.computeIfPresent(node, (k, current) -> current + job.getCpus());
                    break;
                }
            }
        }*/

        /*var nodesToReplicaJobs = new HashMap<Integer, Jobs>();
        for (var node : parameters.getNodes()) {
            nodesToReplicaJobs.put(node, new Jobs(0, new LinkedHashSet<>()));
        }

        for (var nodeAndJobs : nodesToJobs.entrySet()) {
            var node = nodeAndJobs.getKey();
            var nextNode = (node + 1) % parameters.getNodes().size();
            for (var job : nodeAndJobs.getValue().getJobs()) {
                if (job.getRole() == Role.LEADER) {
                    nodesToReplicaJobs.get(nextNode).addStandby(job.getJob());
                }
            }
        }

        logger.info("Total cpu per node: [{}]", cpuPerNodes);

        var result = new HashMap<Integer, List<RoleAwareJob>>();
        for (var nodeAndJobs : nodesToJobs.entrySet()) {
            var leaderJobs = result.computeIfAbsent(nodeAndJobs.getKey(), ignored -> new ArrayList<>());
            leaderJobs.addAll(nodeAndJobs.getValue().getJobs());
        }

        for (var nodeAndJobs : nodesToReplicaJobs.entrySet()) {
            var replicaJobs = result.computeIfAbsent(nodeAndJobs.getKey(), ignored -> new ArrayList<>());
            replicaJobs.addAll(nodeAndJobs.getValue().getJobs());
        }

        tryRebalance(result);

        return result;*/

        return nodesToJobs.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    e -> ImmutableList.copyOf(e.getValue().getJobs()))
            );

    }

    private boolean tryRebalance(Map<Integer, List<RoleAwareJob>> allJobs) {
        if (allJobs.isEmpty() || allJobs.size() == 1) {
            return false;
        }

        var sorted = allJobs.entrySet().stream()
            .sorted(new Comparator<Map.Entry<Integer, List<RoleAwareJob>>>() {
                @Override
                public int compare(Map.Entry<Integer, List<RoleAwareJob>> o1, Map.Entry<Integer, List<RoleAwareJob>> o2) {
                    var s1 = o1.getValue().stream().mapToInt(v -> v.getJob().getCpus()).sum();
                    var s2 = o2.getValue().stream().mapToInt(v -> v.getJob().getCpus()).sum();
                    return Integer.compare(s1, s2);
                }
            })
            .toList();

        var min = 0;
        var max = sorted.size() - 1;

        while (min < max) {
            var minNode = sorted.get(min);
            var maxNode = sorted.get(max);

            // 10, 40, 60
            var maxNodeJobs = maxNode.getValue().stream().filter(j -> j.getRole() == Role.STANDBY)
                .sorted(Comparator.comparing(j -> j.getJob().getCpus()))
                .toList();

            var added = false;
            for (var k = maxNodeJobs.size() - 1; k >= 0; k--) {
                var job = maxNodeJobs.get(k);

                if (!minNode.getValue().contains(job)) {
                    logger.info("Min-max: [{}] [{}]", minNode, maxNode);
                    minNode.getValue().add(job);
                    return true;
                }
            }

            max--;
        }
        //logger.info("[{}]", sorted);
        return false;
    }
}
