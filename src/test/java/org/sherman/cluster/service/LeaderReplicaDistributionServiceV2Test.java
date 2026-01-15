package org.sherman.cluster.service;

import static org.sherman.cluster.domain.Role.LEADER;
import static org.sherman.cluster.domain.Role.STANDBY;

import com.beust.jcommander.internal.Lists;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.RoleAwareJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LeaderReplicaDistributionServiceV2Test {
    private static final Logger logger = LoggerFactory.getLogger(LeaderReplicaDistributionServiceV2Test.class);

    @Test
    public void case1() {
        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    List.of(new Job("index_1", 40), new Job("index_2", 40)),
                    2,
                    List.of(0, 1, 2, 3),
                    90
                ),
                Map.of()
            ),
            Map.of(
                0, List.of(new RoleAwareJob(LEADER, new Job("index_1", 40))),
                1, List.of(new RoleAwareJob(LEADER, new Job("index_2", 40))),
                2, List.of(new RoleAwareJob(STANDBY, new Job("index_1", 40))),
                3, List.of(new RoleAwareJob(STANDBY, new Job("index_2", 40)))
            )
        );
    }

    @Test
    public void case2() {
        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    List.of(
                        new Job("index_1", 40),
                        new Job("index_2", 40),
                        new Job("index_3", 40),
                        new Job("index_4", 40),
                        new Job("index_5", 10)
                    ),
                    2,
                    List.of(0, 1, 2, 3),
                    90
                ),
                Map.of()
            ),
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1", 40)),
                    new RoleAwareJob(LEADER, new Job("index_5", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_4", 40))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_5", 10))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2", 40))
                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_3", 40))
                )
            )
        );
    }

    @Test
    public void case3() {
        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    List.of(
                        new Job("index_1_1", 40),
                        new Job("index_1_2", 40),
                        new Job("index_1_3", 40),
                        new Job("index_1_4", 40),
                        new Job("index_2", 10),
                        new Job("index_3_1", 4),
                        new Job("index_3_2", 4),
                        new Job("index_3_3", 4),
                        new Job("index_3_4", 4),
                        new Job("index_4", 4)
                    ),
                    2,
                    List.of(0, 1, 2, 3),
                    90
                ),
                Map.of()
            ),
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_1", 40)),
                    new RoleAwareJob(LEADER, new Job("index_2", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_1_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_3_3", 4))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_2", 40)),
                    new RoleAwareJob(LEADER, new Job("index_3_1", 4)),
                    new RoleAwareJob(LEADER, new Job("index_3_4", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_3_2", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_4", 4))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_3", 40)),
                    new RoleAwareJob(LEADER, new Job("index_3_2", 4)),
                    new RoleAwareJob(LEADER, new Job("index_4", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_2", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_3_1", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_3_4", 4))

                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_4", 40)),
                    new RoleAwareJob(LEADER, new Job("index_3_3", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_1", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2", 10))
                )
            )
        );
    }

    @Test
    public void case4() {
        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        // first iteration, add 8-shar  d index
        var step1 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("index_1_1", 40),
                    new Job("index_1_2", 40),
                    new Job("index_1_3", 40),
                    new Job("index_1_4", 40),
                    new Job("index_1_5", 40),
                    new Job("index_1_6", 40),
                    new Job("index_1_7", 40),
                    new Job("index_1_8", 40)
                ),
                2,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                90
            ),
            Map.of()
        );

        Assert.assertEquals(
            step1,
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_1", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_3", 40))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_2", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_4", 40))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_5", 40))
                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_6", 40))
                ),
                4, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_5", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_7", 40))
                ),
                5, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_6", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_8", 40))

                ),
                6, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_7", 40))
                ),
                7, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_8", 40))
                ),
                8, List.of(
                    new RoleAwareJob(STANDBY, new Job("index_1_1", 40))
                ),
                9, List.of(
                    new RoleAwareJob(STANDBY, new Job("index_1_2", 40))
                )
            )
        );

        var step2 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("index_1_1", 4),
                    new Job("index_1_2", 4),
                    new Job("index_1_3", 4),
                    new Job("index_1_4", 4),
                    new Job("index_1_5", 4),
                    new Job("index_1_6", 4),
                    new Job("index_1_7", 4),
                    new Job("index_1_8", 4),
                    new Job("index_2_1", 40),
                    new Job("index_2_2", 40),
                    new Job("index_2_3", 40),
                    new Job("index_2_4", 40),
                    new Job("index_2_5", 40),
                    new Job("index_2_6", 40),
                    new Job("index_2_7", 40),
                    new Job("index_2_8", 40)
                ),
                2,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                90
            ),
            step1
        );

        Assert.assertEquals(
            step2,
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_1", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_3", 40))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_2", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_4", 40))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_5", 40))
                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_6", 40))
                ),
                4, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_5", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_7", 40))
                ),
                5, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_6", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_8", 40))

                ),
                6, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_7", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_1", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_3", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_5", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_7", 4))
                ),
                7, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_8", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_2", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_4", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_6", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_8", 4))
                ),
                8, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_1", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_3", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_5", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_7", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_1", 40))
                ),
                9, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_2", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_4", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_6", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_8", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_2", 40))
                )
            )
        );

        var step3 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("index_1_1", 4),
                    new Job("index_1_2", 4),
                    new Job("index_1_3", 4),
                    new Job("index_1_4", 4),
                    new Job("index_1_5", 4),
                    new Job("index_1_6", 4),
                    new Job("index_1_7", 4),
                    new Job("index_1_8", 4),
                    new Job("index_2_1", 40),
                    new Job("index_2_2", 40),
                    new Job("index_2_3", 40),
                    new Job("index_2_4", 40),
                    new Job("index_2_5", 40),
                    new Job("index_2_6", 40),
                    new Job("index_2_7", 40),
                    new Job("index_2_8", 40),
                    new Job("index_3_1", 10),
                    new Job("index_3_2", 10)
                ),
                2,
                List.of(0, 1, 2, 3, 4, 5, 6, 7, 8),
                90
            ),
            step2
        );

        Assert.assertEquals(
            step3,
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_1", 40)),
                    new RoleAwareJob(LEADER, new Job("index_1_6", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_7", 40))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_2", 40)),
                    new RoleAwareJob(LEADER, new Job("index_1_7", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_8", 40))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_3", 40)),
                    new RoleAwareJob(LEADER, new Job("index_1_8", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_3_1", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_3_2", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_1_1", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_2", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_3", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_4", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_5", 4))
                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_1", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_6", 4))
                ),
                4, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_5", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_2", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_7", 4))
                ),
                5, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_6", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_1_8", 4))
                ),
                6, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_7", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_4", 40))
                ),
                7, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_8", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_5", 40))
                ),
                8, List.of(
                    new RoleAwareJob(LEADER, new Job("index_3_1", 10)),
                    new RoleAwareJob(LEADER, new Job("index_3_2", 10)),
                    new RoleAwareJob(LEADER, new Job("index_1_1", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_2", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_3", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_4", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_5", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_6", 40))
                )
            )
        );

        for (var item : step3.values()) {
            var leaderCpu = item.stream().filter(r -> r.getRole() == LEADER).mapToInt(jobs -> jobs.getJob().getCpus()).sum();
            var totalCpu = item.stream().mapToInt(jobs -> jobs.getJob().getCpus()).sum();
            logger.info("[{}] ([{}])", leaderCpu, totalCpu);
        }
    }

    @Test
    public void case5() {
        var nodes = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9); // 10 nodes

        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        var step1 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("ap_1_1", 40),
                    new Job("ap_1_2", 40),
                    new Job("ap_1_3", 40),
                    new Job("ap_1_4", 40),
                    new Job("ap_1_5", 40),
                    new Job("ap_1_6", 40),
                    new Job("ap_1_7", 40),
                    new Job("ap_1_8", 40),
                    new Job("ap_1_9", 10),
                    new Job("ap_1_10", 20),
                    new Job("ap_1_11", 20)
                ),
                2,
                nodes,
                90
            ),
            Map.of()
        );

        for (var nodeDistribution : step1.entrySet()) {
            logger.info("Node: [{}]", nodeDistribution.getKey());
            for (var job : nodeDistribution.getValue()) {
                logger.info("Job: [{}]", job);
            }
        }

        var step2 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("ap_1_1", 40),
                    new Job("ap_1_2", 40),
                    new Job("ap_1_3", 40),
                    new Job("ap_1_4", 40),
                    new Job("ap_1_5", 40),
                    new Job("ap_1_6", 40),
                    new Job("ap_1_7", 40),
                    new Job("ap_1_8", 40),
                    new Job("ap_1_9", 10),
                    new Job("ap_1_10", 10),
                    new Job("ap_1_11", 20),
                    new Job("ap_2_1", 40),
                    new Job("ap_2_2", 40),
                    new Job("ap_2_3", 40),
                    new Job("ap_2_4", 40),
                    new Job("ap_2_5", 40),
                    new Job("ap_2_6", 40),
                    new Job("ap_2_7", 40),
                    new Job("ap_2_8", 40),
                    new Job("ap_2_9", 10),
                    new Job("ap_2_10", 20),
                    new Job("ap_2_11", 20)
                ),
                2,
                nodes,
                90
            ),
            step1
        );

        for (var nodeDistribution : step1.entrySet()) {
            logger.info("Node: [{}]", nodeDistribution.getKey());
            var state1 = Lists.newArrayList(nodeDistribution.getValue());
            var state2 = Lists.newArrayList(step2.get(nodeDistribution.getKey()));
            Collections.sort(state1, COMPARATOR);
            Collections.sort(state2, COMPARATOR);
            logger.info("old: [{}]", state1);
            logger.info("new: [{}]", state2);
        }
    }

    @Test
    public void case6() {
        var nodes = List.of(0, 1, 2); // 3 nodes

        var distributionService = new LeaderReplicaDistributionServiceImplV2();

        var step1 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("ap_1_1", 40),
                    new Job("ap_1_2", 40),
                    new Job("ap_1_3", 40)
                ),
                2,
                nodes,
                90
            ),
            Map.of()
        );

        for (var nodeDistribution : step1.entrySet()) {
            logger.info("Node: [{}]", nodeDistribution.getKey());
            for (var job : nodeDistribution.getValue()) {
                logger.info("Job: [{}]", job);
            }
        }

        /*var step2 = distributionService.distribute(
            new LeaderReplicaDistribution(
                List.of(
                    new Job("ap_1_1", 40),
                    new Job("ap_1_2", 40),
                    new Job("ap_1_3", 40),
                    new Job("ap_1_4", 40),
                    new Job("ap_1_5", 40),
                    new Job("ap_1_6", 40),
                    new Job("ap_1_7", 40),
                    new Job("ap_1_8", 40),
                    new Job("ap_1_9", 10),
                    new Job("ap_1_10", 10),
                    new Job("ap_1_11", 20),
                    new Job("ap_2_1", 40),
                    new Job("ap_2_2", 40),
                    new Job("ap_2_3", 40),
                    new Job("ap_2_4", 40),
                    new Job("ap_2_5", 40),
                    new Job("ap_2_6", 40),
                    new Job("ap_2_7", 40),
                    new Job("ap_2_8", 40),
                    new Job("ap_2_9", 10),
                    new Job("ap_2_10", 20),
                    new Job("ap_2_11", 20)
                ),
                2,
                nodes,
                90
            ),
            step1
        );

        for (var nodeDistribution : step1.entrySet()) {
            logger.info("Node: [{}]", nodeDistribution.getKey());
            var state1 = Lists.newArrayList(nodeDistribution.getValue());
            var state2 = Lists.newArrayList(step2.get(nodeDistribution.getKey()));
            Collections.sort(state1, COMPARATOR);
            Collections.sort(state2, COMPARATOR);
            logger.info("old: [{}]", state1);
            logger.info("new: [{}]", state2);
        }*/
    }

    private static final Comparator<RoleAwareJob> COMPARATOR = Comparator.comparing(o -> o.getJob().getId());
}
