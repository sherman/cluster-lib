package org.sherman.cluster.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Random;

public class DistributedCacheServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(DistributedCacheServiceTest.class);

    private final int SERVERS = 128;
    private final int PARTITIONS = 16;


    @Test
    public void baseLine() {
        DistributedCacheService distributedCacheService = new DistributedCacheServiceImpl(SERVERS, PARTITIONS);
        Random random = new Random();
        for (int i = 0; i < SERVERS * PARTITIONS  * 10; i++) {
            int id = random.nextInt(SERVERS * PARTITIONS);
            int serverId = Math.max(1, random.nextInt(SERVERS + 1));
            distributedCacheService.cache(serverId, String.valueOf(id), -1);
        }
        logger.info("Total: [{}] ([{}])", distributedCacheService.getPercentOfUnique(), SERVERS * PARTITIONS);
    }

    @Test
    public void sharded() {
        DistributedCacheService distributedCacheService = new DistributedCacheServiceImpl(SERVERS, PARTITIONS);
        Random random = new Random();
        for (int i = 0; i < SERVERS * PARTITIONS * 10; i++) {
            int id = random.nextInt(SERVERS * PARTITIONS);
            int serverId = Math.max(1, random.nextInt(SERVERS + 1));
            int partition = Math.max(1, random.nextInt(PARTITIONS + 1));
            distributedCacheService.cache(serverId, String.valueOf(id), partition);
        }
        logger.info("Total: [{}] ([{}])", distributedCacheService.getPercentOfUnique(), SERVERS * PARTITIONS);
    }
}
