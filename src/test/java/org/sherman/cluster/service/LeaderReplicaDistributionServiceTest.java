package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sherman.cluster.domain.Job;
import org.sherman.cluster.domain.LeaderReplicaDistribution;
import org.sherman.cluster.domain.Role;
import org.sherman.cluster.domain.RoleAwareJob;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LeaderReplicaDistributionServiceTest {
    @Test
    public void case1() {
        LeaderReplicaDistributionService distributionService = new LeaderReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    ImmutableList.of(new Job("index_1", 40), new Job("index_2", 40)),
                    2,
                    ImmutableList.of(0, 1, 2, 3),
                    90
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(new RoleAwareJob(Role.LEADER, new Job("index_1", 40))),
                1, ImmutableList.of(new RoleAwareJob(Role.LEADER, new Job("index_2", 40))),
                2, ImmutableList.of(new RoleAwareJob(Role.STANDBY, new Job("index_1", 40))),
                3, ImmutableList.of(new RoleAwareJob(Role.STANDBY, new Job("index_2", 40)))
            )
        );
    }

    @Test
    public void case2() {
        LeaderReplicaDistributionService distributionService = new LeaderReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    ImmutableList.of(
                        new Job("index_1", 40),
                        new Job("index_2", 40),
                        new Job("index_3", 40),
                        new Job("index_4", 40),
                        new Job("index_5", 10)
                    ),
                    2,
                    ImmutableList.of(0, 1, 2, 3),
                    90
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_5", 10)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_4", 40))
                ),
                1, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_2", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_5", 10))
                ),
                2, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_3", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_2", 40))
                ),
                3, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_4", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3", 40))
                )
            )
        );
    }

    @Test
    public void case3() {
        LeaderReplicaDistributionService distributionService = new LeaderReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new LeaderReplicaDistribution(
                    ImmutableList.of(
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
                    ImmutableList.of(0, 1, 2, 3),
                    90
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_1", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_2", 10)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_4", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_3", 4))
                ),
                1, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_2", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_1", 4)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_4", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_3", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_2", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_4", 4))
                ),
                2, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_3", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_2", 4)),
                    new RoleAwareJob(Role.LEADER, new Job("index_4", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_2", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_1", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_3_4", 4))

                ),
                3, ImmutableList.of(
                    new RoleAwareJob(Role.LEADER, new Job("index_1_4", 40)),
                    new RoleAwareJob(Role.LEADER, new Job("index_3_3", 4)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_1_1", 40)),
                    new RoleAwareJob(Role.STANDBY, new Job("index_2", 10))
                )
            )
        );
    }
}
