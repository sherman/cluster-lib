package org.sherman.cluster.balancing;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ShardBalancerPropertyTest {
    @Test
    public void runQuickcheckProperties() {
        Result result = JUnitCore.runClasses(ShardBalancerQuickcheckProperties.class);
        Assert.assertTrue(result.wasSuccessful(), result.getFailures().toString());
    }
}
