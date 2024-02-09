package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaDistributionServiceV2Test {
    @Test
    public void case1() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(0),
                1, ImmutableList.of(1),
                2, ImmutableList.of(0),
                3, ImmutableList.of(1)
            )
        );
    }

    @Test
    public void case3() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(0),
                1, ImmutableList.of(1),
                2, ImmutableList.of(0),
                3, ImmutableList.of(1)
            )
        );
    }

    @Test
    public void case4() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

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
                .put(1, ImmutableList.of(1, 2))
                .put(2, ImmutableList.of(2, 0))
                .put(3, ImmutableList.of(0, 1))
                .put(4, ImmutableList.of(1, 2))
                .put(5, ImmutableList.of(2, 0))
                .put(6, ImmutableList.of(0, 1))
                .build()
        );
    }

    @Test
    public void case5() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

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
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    2,
                    ImmutableList.of(0, 1)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(0, 1),
                1, ImmutableList.of(1, 0)
            )
        );
    }

    @Test
    public void case7() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1),
                    3,
                    ImmutableList.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, ImmutableList.of(0),
                1, ImmutableList.of(1, 0),
                2, ImmutableList.of(0, 1),
                3, ImmutableList.of(1)
            )
        );
    }

    @Test
    public void case8() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(0, 6))
                .put(1, ImmutableList.of(1, 0))
                .put(2, ImmutableList.of(2, 1))
                .put(3, ImmutableList.of(3, 2))
                .put(4, ImmutableList.of(4, 3))
                .put(5, ImmutableList.of(5, 4))
                .put(6, ImmutableList.of(6, 5))
                .build()
        );
    }

    @Test
    public void case9() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    ImmutableList.of(0, 1, 2)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(0, 3, 6, 2, 5))
                .put(1, ImmutableList.of(1, 4, 0, 3, 6))
                .put(2, ImmutableList.of(2, 5, 1, 4))
                .build()
        );
    }

    /**
     * case 9 with removed node 3.
     */
    @Test
    public void case10() {
        ReplicaDistributionService distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    ImmutableList.of(0, 1, 2, 4, 5, 6),
                    2,
                    ImmutableList.of(0, 1, 2)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, ImmutableList.of(0, 4, 2, 6))
                .put(1, ImmutableList.of(1, 5, 0, 4))
                .put(2, ImmutableList.of(2, 6, 1, 5))
                .build()
        );
    }
}
