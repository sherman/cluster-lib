package org.sherman.cluster.service;

import static org.sherman.cluster.domain.Role.LEADER;
import static org.sherman.cluster.domain.Role.STANDBY;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.RoleAwareJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SwapTest {
    private static final Logger logger = LoggerFactory.getLogger(SwapTest.class);

    @Test
    public void case1() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(2, 40), new Job(3, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(1, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case2() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case3() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(2, 40)));
        result.put(2, Lists.newArrayList());
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case4() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(2, 40), new Job(3, 40), new Job(4, 40), new Job(5, 40), new Job(6, 40)));
        result.put(2, Lists.newArrayList());
        result.put(3, Lists.newArrayList());

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case5() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case6() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(4, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case7() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(9, 40), new Job(17, 40), new Job(18, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(10, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(11, 40)));
        result.put(4, Lists.newArrayList(new Job(4, 40), new Job(12, 40), new Job(19, 40), new Job(20, 40)));
        result.put(5, Lists.newArrayList(new Job(5, 40), new Job(13, 40)));
        result.put(6, Lists.newArrayList());
        result.put(7, Lists.newArrayList());
        result.put(8, Lists.newArrayList(new Job(8, 40), new Job(16, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void case8() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(9, 10), new Job(10, 10), new Job(11, 30), new Job(12, 30)));
        result.put(2, Lists.newArrayList(new Job(2, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));
        result.put(4, Lists.newArrayList(new Job(4, 40)));
        result.put(5, Lists.newArrayList(new Job(5, 40)));
        result.put(6, Lists.newArrayList(new Job(6, 40)));
        result.put(7, Lists.newArrayList(new Job(7, 40)));
        result.put(8, Lists.newArrayList(new Job(8, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas.
     */
    @Test
    public void case9() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(1, 40), new Job(2, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(3, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas.
     */
    @Test
    public void case10() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(3, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(1, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(2, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas.
     */
    @Test
    public void case11() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(3, 40), new Job(4, 40), new Job(6, 40)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(1, 40), new Job(5, 40), new Job(4, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(2, 40), new Job(6, 40), new Job(5, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas. Rebalance, step 1 (remove a node)
     */
    @Test
    public void case12() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(3, 40), new Job(4, 40), new Job(6, 40), new Job(2, 40), new Job(5, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(2, 40), new Job(6, 40), new Job(5, 40), new Job(1, 40), new Job(4, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas. Rebalance, step 2 (got back the node)
     */
    @Test
    public void case13() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList(new Job(1, 40), new Job(3, 40), new Job(4, 40), new Job(6, 40), new Job(2, 40), new Job(5, 40)));
        result.put(2, Lists.newArrayList());
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(2, 40), new Job(6, 40), new Job(5, 40), new Job(1, 40), new Job(4, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    /**
     * With replicas.
     */
    @Test
    public void case14() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1,
            Lists.newArrayList(new Job(1, 40), new Job(3, 40), new Job(4, 4), new Job(4, 4), new Job(5, 4), new Job(5, 4), new Job(6, 4),
                new Job(6, 4)));
        result.put(2, Lists.newArrayList(new Job(2, 40), new Job(1, 40)));
        result.put(3, Lists.newArrayList(new Job(3, 40), new Job(2, 40)));

        print(result);
        rebalance(result);
        print(result);
    }

    @Test
    public void simulate() {
        var result = new HashMap<Integer, List<Job>>();
        result.put(1, Lists.newArrayList());
        result.put(2, Lists.newArrayList());
        result.put(3, Lists.newArrayList());
        result.put(4, Lists.newArrayList());
        result.put(5, Lists.newArrayList());
        result.put(6, Lists.newArrayList());
        result.put(7, Lists.newArrayList());
        result.put(8, Lists.newArrayList());
        result.put(9, Lists.newArrayList());
        result.put(10, Lists.newArrayList());

        var allJobs = new ArrayList<org.sherman.cluster.domain.Job>();
        var random = new Random();
        var offset = 0;
        for (var i = 0; i < 10; i++) {
            logger.info("new index started");
            var jobs = randomIndex(offset, 8);
            offset += jobs.size();
            for (var k = 0; k < jobs.size(); k++) {
                var job = jobs.get(k);
                var randomNode = random.nextInt(1, 11);
                result.get(randomNode).add(job);
                allJobs.add(new org.sherman.cluster.domain.Job(String.valueOf(job.id()), job.cpus()));
                logger.info("new node added");
                print(result);
                rebalance(result);
                print(result);
            }
        }

        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        var distribution = distributionService.distribute(
            new LeaderReplicaDistribution(
                allJobs,
                1,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                150
            ),
            Map.of()
        );
        printDistributed(distribution);
    }

    public List<Job> randomIndex(int offset, int maxShards) {
        var random = new Random();
        var shards = random.nextInt(1, maxShards + 1);
        var cpu = random.nextInt(2, 41);
        var jobs = new ArrayList<Job>();
        for (var i = 0; i < shards; i++) {
            jobs.add(new Job(offset + i, cpu));
        }
        return jobs;
    }

    private void rebalance(Map<Integer, List<Job>> unbalanced) {
        if (unbalanced.size() < 2) {
            return;
        }

        while (true) {
            if (!doRebalance(unbalanced)) {
                return;
            }
        }
    }

    private boolean doRebalance(Map<Integer, List<Job>> unbalanced) {
        var sorted = unbalanced.entrySet().stream()
            .sorted(new Comparator<Map.Entry<Integer, List<Job>>>() {
                @Override
                public int compare(Map.Entry<Integer, List<Job>> o1, Map.Entry<Integer, List<Job>> o2) {
                    var s1 = o1.getValue().stream().mapToInt(Job::cpus).sum();
                    var s2 = o2.getValue().stream().mapToInt(Job::cpus).sum();
                    var sumRes = Integer.compare(s1, s2);
                    return (sumRes != 0) ? sumRes : Integer.compare(o1.getKey(), o2.getKey());
                }
            })
            .toList();

        var minIndex = 0;
        var maxIndex = sorted.size() - 1;


        while (minIndex >= 0 && minIndex < maxIndex) {
            var max = sorted.get(maxIndex);
            var min = sorted.get(minIndex);

            var maxJobs = max.getValue().stream()
                .sorted(Comparator.comparing(Job::cpus).reversed())
                .toList();

            for (var job : maxJobs) {
                var totalMin = min.getValue().stream().mapToInt(Job::cpus).sum();
                var totalMax = max.getValue().stream().mapToInt(Job::cpus).sum();
                if ((totalMin + job.cpus()) <= (totalMax - job.cpus())) {
                    if (!min.getValue().contains(job)) {
                        min.getValue().add(job);
                        max.getValue().remove(job);
                        logger.info("Swap: [{}] -> [{}] (job: [{}])", max.getKey(), min.getKey(), job);
                        return true;
                    }
                } else {
                    /*var minJobs = min.getValue().stream()
                        .sorted(Comparator.comparing(Job::cpus))
                        .toList();

                    var minMinJob = minJobs.size() > 1 ? minJobs.getFirst() : null;

                    if (minMinJob != null) {
                        if ((totalMin + job.cpus() - minMinJob.cpus()) <= (totalMax - job.cpus() + minMinJob.cpus())) {
                            min.getValue().add(job);
                            max.getValue().remove(job);

                            max.getValue().add(minMinJob);
                            min.getValue().remove(minMinJob);

                            logger.info("Swap: [{}] -> [{}] (job: [{}])", min.getKey(), max.getKey(), minMinJob);
                            return true;
                        }
                    }*/
                }
            }
            maxIndex--;
        }

        return false;
    }

    private void print(Map<Integer, List<Job>> balanced) {
        for (var nodeAndJobs : balanced.entrySet()) {
            var sum = 0;
            logger.info("Node: [{}]", nodeAndJobs.getKey());
            for (var job : nodeAndJobs.getValue()) {
                logger.info("Job: [{}]", job);
                sum += job.cpus();
            }
            logger.info("Node: [{}] ({})", nodeAndJobs.getKey(), sum);
        }
        logger.info("============================================");
    }

    private void printDistributed(Map<Integer, List<RoleAwareJob>> distribution) {
        for (var nodeAndJobs : distribution.entrySet()) {
            var sum = 0;
            logger.info("Node: [{}]", nodeAndJobs.getKey());
            for (var job : nodeAndJobs.getValue()) {
                logger.info("Job: [{}]", job);
                sum += job.getJob().getCpus();
            }
            logger.info("Node: [{}] ({})", nodeAndJobs.getKey(), sum);
        }
        logger.info("============================================");
    }

    private record Job(int id, int cpus) {
    }
}
