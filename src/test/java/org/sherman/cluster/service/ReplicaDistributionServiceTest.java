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
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, List.of(1),
                1, List.of(0),
                2, List.of(1),
                3, List.of(0)
            )
        );
    }

    @Test
    public void case3() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, List.of(1),
                1, List.of(0),
                2, List.of(1),
                3, List.of(0)
            )
        );
    }

    @Test
    public void case4() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2),
                    5,
                    List.of(),
                    List.of(0, 1, 2, 3, 4, 5, 6)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 1, 2))
                .put(1, List.of(2, 1, 0))
                .put(2, List.of(1, 0, 2))
                .put(3, List.of(2, 0, 1))
                .put(4, List.of(0, 2, 1))
                .put(5, List.of(0, 1, 2))
                .put(6, List.of(2, 1, 0))
                .build()
        );
    }

    @Test
    public void case5() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0),
                    1,
                    List.of(),
                    List.of(0)
                )
            ),
            ImmutableMap.of(0, List.of(0))
        );
    }

    @Test
    public void case6() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1)
                )
            ),
            ImmutableMap.of(
                0, List.of(1, 0),
                1, List.of(0, 1)
            )
        );
    }

    @Test
    public void case7() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    3,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            ImmutableMap.of(
                0, List.of(1, 0),
                1, List.of(0, 1),
                2, List.of(1, 0),
                3, List.of(0, 1)
            )
        );
    }

    @Test
    public void case8() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3, 4, 5, 6)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(2, 1))
                .put(1, List.of(4, 3))
                .put(2, List.of(6, 5))
                .put(3, List.of(0, 1))
                .put(4, List.of(2, 3))
                .put(5, List.of(4, 5))
                .put(6, List.of(0, 6))
                .build()
        );
    }

    @Test
    public void case9() {
        var distributionService = new ReplicaDistributionServiceImpl();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6),
                    2,
                    List.of(),
                    List.of(0, 1, 2)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(6, 3, 4, 2, 5))
                .put(1, List.of(1, 5, 3, 4, 6))
                .put(2, List.of(6, 3, 4, 2, 5))
                .build()
        );
    }
}
