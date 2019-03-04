package org.sherman.cluster.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Denis M. Gabaydulin
 * @since 04.03.19
 */
public class HbaseStyleStorageTest {
    private static final Logger log = LoggerFactory.getLogger(HbaseStyleStorageTest.class);

    @Test
    public void splitExample() {
        ServerStorage serverStorage = new ServerStorageImpl(
            ImmutableList.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        HbaseStyleStorage<Long> hbaseStorage = new HbaseStyleStorageImpl(serverStorage, Range.closed(0L, Long.MAX_VALUE), 1024);

        for (int i = 0; i < 1024 * 1024; i++) {
            hbaseStorage.putKey((long) i);

            if (i % 1024 == 0) {
                log.info("{}", hbaseStorage.getDistribution());
            }
        }

        log.info("{}", hbaseStorage.getDistribution());
    }
}
