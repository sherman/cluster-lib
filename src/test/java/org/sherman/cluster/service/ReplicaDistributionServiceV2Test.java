package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaDistributionServiceV2Test {
    @Test
    public void case1() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            Map.of(
                0, List.of(0),
                1, List.of(1),
                2, List.of(0),
                3, List.of(1)
            )
        );
    }

    @Test
    public void case3() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            Map.of(
                0, List.of(0),
                1, List.of(1),
                2, List.of(0),
                3, List.of(1)
            )
        );
    }

    @Test
    public void case4() {
        var distributionService = new ReplicaDistributionServiceImplV2();

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
                .put(1, List.of(1, 2))
                .put(2, List.of(2, 0))
                .put(3, List.of(0, 1))
                .put(4, List.of(1, 2))
                .put(5, List.of(2, 0))
                .put(6, List.of(0, 1))
                .build()
        );
    }

    @Test
    public void case5() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0),
                    1,
                    List.of(),
                    List.of(0)
                )
            ),
            Map.of(0, List.of(0))
        );
    }

    @Test
    public void case6() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    2,
                    List.of(),
                    List.of(0, 1)
                )
            ),
            Map.of(
                0, List.of(0, 1),
                1, List.of(1, 0)
            )
        );
    }

    @Test
    public void case7() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1),
                    3,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            Map.of(
                0, List.of(0),
                1, List.of(1, 0),
                2, List.of(0, 1),
                3, List.of(1)
            )
        );
    }

    @Test
    public void case8() {
        var distributionService = new ReplicaDistributionServiceImplV2();

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
                .put(0, List.of(0, 6))
                .put(1, List.of(1, 0))
                .put(2, List.of(2, 1))
                .put(3, List.of(3, 2))
                .put(4, List.of(4, 3))
                .put(5, List.of(5, 4))
                .put(6, List.of(6, 5))
                .build()
        );
    }

    @Test
    public void case9() {
        var distributionService = new ReplicaDistributionServiceImplV2();

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
                .put(0, List.of(0, 3, 6, 2, 5))
                .put(1, List.of(1, 4, 0, 3, 6))
                .put(2, List.of(2, 5, 1, 4))
                .build()
        );
    }

    /**
     * Remove one node.
     */
    @Test
    public void case10() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 4),
                    2,
                    List.of(),
                    List.of(0, 1, 2, 3)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 4))
                .put(1, List.of(1, 0))
                .put(2, List.of(2, 1))
                .put(3, List.of(4, 2))
                .build()
        );

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 4),
                    2,
                    List.of(),
                    List.of(0, 1, 2)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 4, 2))
                .put(1, List.of(1, 0, 4))
                .put(2, List.of(2, 1))
                .build()
        );
    }

    /**
     * Remove node 9, only two nodes are re-balancing.
     */
    @Test
    public void case11() {
        var distributionService = new ReplicaDistributionServiceImplV2();

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    4,
                    List.of(),
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 7))
                .put(1, List.of(1, 0))
                .put(2, List.of(2, 1))
                .put(3, List.of(3, 2))
                .put(4, List.of(4, 3))
                .put(5, List.of(5, 4))
                .put(6, List.of(6, 5))
                .put(7, List.of(7, 6))
                .put(8, List.of(0, 7))
                .put(9, List.of(1, 0))
                .put(10, List.of(2, 1))
                .put(11, List.of(3, 2))
                .put(12, List.of(4, 3))
                .put(13, List.of(5, 4))
                .put(14, List.of(6, 5))
                .put(15, List.of(7, 6))
                .build()
        );

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    4,
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 7, 6))
                .put(1, List.of(1, 0, 7))
                .put(2, List.of(2, 1))
                .put(3, List.of(3, 2))
                .put(4, List.of(4, 3))
                .put(5, List.of(5, 4))
                .put(6, List.of(6, 5))
                .put(7, List.of(7, 6))
                .put(8, List.of(0, 7))
                .put(10, List.of(2, 1))
                .put(11, List.of(3, 2))
                .put(12, List.of(4, 3))
                .put(13, List.of(5, 4))
                .put(14, List.of(6, 5))
                .put(15, List.of(1, 0))
                .build()
        );

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    4,
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 10, 11, 12, 13, 14),
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 10, 11, 12, 13, 14, 16)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 7))
                .put(1, List.of(1, 0))
                .put(2, List.of(2, 1))
                .put(3, List.of(3, 2))
                .put(4, List.of(4, 3))
                .put(5, List.of(5, 4))
                .put(6, List.of(6, 5))
                .put(7, List.of(7, 6))
                .put(8, List.of(0, 7))
                .put(10, List.of(2, 1))
                .put(11, List.of(3, 2))
                .put(12, List.of(4, 3))
                .put(13, List.of(5, 4))
                .put(14, List.of(6, 5))
                .put(15, List.of(1, 0))
                .put(16, List.of(7, 6))
                .build()
        );

        Assert.assertEquals(
            distributionService.distribute(
                new ReplicaDistribution(
                    List.of(0, 1, 2, 3, 4, 5, 6, 7),
                    5,
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 10, 11, 12, 13, 14, 16),
                    List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 10, 11, 12, 13, 14, 16)
                )
            ),
            new ImmutableMap.Builder<>()
                .put(0, List.of(0, 7))
                .put(1, List.of(1, 0))
                .put(2, List.of(2, 1, 0))
                .put(3, List.of(3, 2, 1))
                .put(4, List.of(4, 3, 2))
                .put(5, List.of(5, 4, 3))
                .put(6, List.of(6, 5, 4))
                .put(7, List.of(7, 6, 5))
                .put(8, List.of(0, 7, 6))
                .put(10, List.of(2, 1))
                .put(11, List.of(3, 2))
                .put(12, List.of(4, 3))
                .put(13, List.of(5, 4))
                .put(14, List.of(6, 5))
                .put(15, List.of(1, 0, 7))
                .put(16, List.of(7, 6))
                .build()
        );
    }
}
