package org.sherman.cluster.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PermutationUtilsTest {
    @Test
    public void getPermutations() {
        Assert.assertEquals(
            PermutationUtils.getPermutations(ImmutableList.of(1, 2, 3), 2),
            ImmutableSet.of(
                ImmutableList.of(2, 1),
                ImmutableList.of(3, 2),
                ImmutableList.of(1, 2),
                ImmutableList.of(2, 3),
                ImmutableList.of(1, 3),
                ImmutableList.of(3, 1)
            )
        );

        Assert.assertEquals(
            PermutationUtils.getPermutations(ImmutableList.of(1, 2), 1),
            ImmutableSet.of(
                ImmutableList.of(1),
                ImmutableList.of(2)
            )
        );

        Assert.assertEquals(
            PermutationUtils.getPermutations(ImmutableList.of(1, 2, 3), 3),
            ImmutableSet.of(
                ImmutableList.of(1, 2, 3),
                ImmutableList.of(3, 2, 1),
                ImmutableList.of(2, 1, 3),
                ImmutableList.of(3, 1, 2),
                ImmutableList.of(1, 3, 2),
                ImmutableList.of(2, 3, 1)
            )
        );
    }
}
