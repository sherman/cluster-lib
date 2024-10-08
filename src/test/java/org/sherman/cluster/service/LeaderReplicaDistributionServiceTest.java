package org.sherman.cluster.service;

import static org.sherman.cluster.domain.Role.LEADER;
import static org.sherman.cluster.domain.Role.STANDBY;

import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.Role;
import org.sherman.cluster.domain.RoleAwareJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LeaderReplicaDistributionServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(LeaderReplicaDistributionServiceTest.class);

    @Test
    public void case1() {
        var distributionService = new LeaderReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    List.of(new Job("index_1", 40), new Job("index_2", 40)),
                    2,
                    List.of(0, 1, 2, 3),
                    90
                )
            ),
            Map.of(
                0, List.of(new RoleAwareJob(Role.LEADER, new Job("index_1", 40))),
                1, List.of(new RoleAwareJob(Role.LEADER, new Job("index_2", 40))),
                2, List.of(new RoleAwareJob(Role.STANDBY, new Job("index_1", 40))),
                3, List.of(new RoleAwareJob(Role.STANDBY, new Job("index_2", 40)))
            )
        );
    }

    @Test
    public void case2() {
        var distributionService = new LeaderReplicaDistributionServiceImpl();

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
                )
            ),
            Map.of(
                0, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_5", 10)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_4", 40))
                ),
                1, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_2", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_5", 10))
                ),
                2, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_3", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_2", 40))
                ),
                3, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_4", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3", 40))
                )
            )
        );
    }

    @Test
    public void case3() {
        var distributionService = new LeaderReplicaDistributionServiceImpl();

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
                )
            ),
            Map.of(
                0, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_1", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_2", 10)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_4", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_3", 4))
                ),
                1, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_2", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_1", 4)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_4", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_3", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_2", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_4", 4))
                ),
                2, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_3", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_2", 4)),
                    new RoleAwareJob(Role.LEADER, new Job("index_4", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_2", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_1", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_4", 4))

                ),
                3, List.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_4", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_3", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_1", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_2", 10))
                )
            )
        );
    }

    @Test
    public void case4() {
        var distributionService = new LeaderReplicaDistributionServiceImpl();

        // first iteration, add 8-shard index
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
            )
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
            )
        );

        Assert.assertEquals(
            step2,
            Map.of(
                0, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_1", 4)),
                    new RoleAwareJob(LEADER, new Job("index_2_3", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_5", 40))
                ),
                1, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_2", 4)),
                    new RoleAwareJob(LEADER, new Job("index_2_4", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_6", 40))
                ),
                2, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_3", 4)),
                    new RoleAwareJob(LEADER, new Job("index_2_5", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_7", 40))
                ),
                3, List.of(
                    new RoleAwareJob(LEADER, new Job("index_1_4", 4)),
                    new RoleAwareJob(LEADER, new Job("index_2_6", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_2_8", 40))
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
            )
        );

        Assert.assertEquals(
            step3,
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
                    new RoleAwareJob(STANDBY, new Job("index_3_1", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_1_1", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_3", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_5", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_7", 4))
                ),
                7, List.of(
                    new RoleAwareJob(LEADER, new Job("index_2_8", 40)),
                    new RoleAwareJob(STANDBY, new Job("index_3_2", 10)),
                    new RoleAwareJob(STANDBY, new Job("index_1_2", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_4", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_6", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_1_8", 4))
                ),
                8, List.of(
                    new RoleAwareJob(LEADER, new Job("index_3_1", 10)),
                    new RoleAwareJob(LEADER, new Job("index_1_1", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_3", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_5", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_7", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_1", 40))
                ),
                9, List.of(
                    new RoleAwareJob(LEADER, new Job("index_3_2", 10)),
                    new RoleAwareJob(LEADER, new Job("index_1_2", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_4", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_6", 4)),
                    new RoleAwareJob(LEADER, new Job("index_1_8", 4)),
                    new RoleAwareJob(STANDBY, new Job("index_2_2", 40))
                )
            )
        );

        for (var item : step3.values()) {
            var leaderCpu = item.stream().filter(r -> r.getRole() == LEADER).mapToInt(jobs -> jobs.getJob().getCpus()).sum();
            var totalCpu = item.stream().mapToInt(jobs -> jobs.getJob().getCpus()).sum();
            logger.info("[{}] ([{}])", leaderCpu, totalCpu);
        }
    }
}
