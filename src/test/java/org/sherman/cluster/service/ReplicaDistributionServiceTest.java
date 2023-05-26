package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaDistributionServiceTest {
    @Test
    public void case1() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(1),
                1, ImmutableList.of(0),
                2, ImmutableList.of(1),
                3, ImmutableList.of(0)
            )
        );
    }

    @Test
    public void case3() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(1),
                1, ImmutableList.of(0),
                2, ImmutableList.of(1),
                3, ImmutableList.of(0)
            )
        );
    }

    @Test
    public void case4() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2),
                    5,
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(0, 1, 2))
                .put(1, ImmutableList.of(2, 1, 0))
                .put(2, ImmutableList.of(1, 0, 2))
                .put(3, ImmutableList.of(2, 0, 1))
                .put(4, ImmutableList.of(0, 2, 1))
                .put(5, ImmutableList.of(0, 1, 2))
                .put(6, ImmutableList.of(2, 1, 0))
                .build()
        );
    }

    @Test
    public void case5() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0),
                    1,
                    ImmutableList.of(0)
                )
            ),
            ImmutableMap.of(0, ImmutableList.of(0))
        );
    }

    @Test
    public void case6() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(1, 0),
                1, ImmutableList.of(0, 1)
            )
        );
    }

    @Test
    public void case7() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    3,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(1, 0),
                1, ImmutableList.of(0, 1),
                2, ImmutableList.of(1, 0),
                3, ImmutableList.of(0, 1)
            )
        );
    }

    @Test
    public void case8() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(2, 1))
                .put(1, ImmutableList.of(4, 3))
                .put(2, ImmutableList.of(6, 5))
                .put(3, ImmutableList.of(0, 1))
                .put(4, ImmutableList.of(2, 3))
                .put(5, ImmutableList.of(4, 5))
                .put(6, ImmutableList.of(0, 6))
                .build()
        );
    }

    @Test
    public void case9() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    ImmutableList.of(0, 1, 2)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(6, 3, 4, 2, 5))
                .put(1, ImmutableList.of(1, 5, 3, 4, 6))
                .put(2, ImmutableList.of(6, 3, 4, 2, 5))
                .build()
        );
    }
}
