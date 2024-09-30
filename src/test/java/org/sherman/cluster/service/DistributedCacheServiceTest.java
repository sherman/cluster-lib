package org.sherman.cluster.service;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class DistributedCacheServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(DistributedCacheServiceTest.class);

    private final int SERVERS = 128;
    private final int PARTITIONS = 16;


    @Test
    public void baseLine() {
        var distributedCacheService = new DistributedCacheServiceImpl(SERVERS, PARTITIONS);
        var random = new Random();
        for (var i = 0; i < SERVERS * PARTITIONS * 10; i++) {
            var id = random.nextInt(SERVERS * PARTITIONS);
            var serverId = Math.max(1, random.nextInt(SERVERS + 1));
            distributedCacheService.cache(serverId, String.valueOf(id), -1);
        }
        logger.info("Total: [{}] ([{}])", distributedCacheService.getPercentOfUnique(), SERVERS * PARTITIONS);
    }

    @Test
    public void sharded() {
        var distributedCacheService = new DistributedCacheServiceImpl(SERVERS, PARTITIONS);
        var random = new Random();
        for (var i = 0; i < SERVERS * PARTITIONS * 10; i++) {
            var id = random.nextInt(SERVERS * PARTITIONS);
            var serverId = Math.max(1, random.nextInt(SERVERS + 1));
            var partition = Math.max(1, random.nextInt(PARTITIONS + 1));
            distributedCacheService.cache(serverId, String.valueOf(id), partition);
        }
        logger.info("Total: [{}] ([{}])", distributedCacheService.getPercentOfUnique(), SERVERS * PARTITIONS);
    }
}
