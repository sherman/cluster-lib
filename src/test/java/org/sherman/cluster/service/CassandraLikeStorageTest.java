package org.sherman.cluster.service;

import com.google.common.collect.Range;
import java.util.List;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * @author Denis M. Gabaydulin
 * @since 08.03.19
 */
public class CassandraLikeStorageTest {
    private static final Logger log = LoggerFactory.getLogger(CassandraLikeStorageTest.class);

    @Test
    public void smallTokenNumber() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var storage = new CassandraLikeStorageImpl(serverStorage, Range.closed(0L, 100L), 4);
    }

    @Test
    public void initialTokenGeneration() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var storage = new CassandraLikeStorageImpl(serverStorage, Range.closed(Long.MIN_VALUE, Long.MAX_VALUE), 256);

        for (var i = 0; i < 1024 * 1024; i++) {
            storage.putKey(String.valueOf(i));

            if (i % 1024 == 0) {
                log.info("{}", storage.getDistribution());
            }
        }

        log.info("{}", storage.getDistribution());
    }

    @Test
    public void rebalance() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var storage = new CassandraLikeStorageImpl(serverStorage, Range.closed(Long.MIN_VALUE, Long.MAX_VALUE), 256);
        //CassandraLikeStorage<Long> storage = new CassandraLikeStorageImpl(serverStorage, Range.closed(0L, 1000000L), 16);

        log.info("Ranges per node: {}", storage.getRangeDistribution());

        storage.addServer(new ServerNode("4", "192.168.5.4"));

        log.info("Ranges per node: {}", storage.getRangeDistribution());

        for (var i = 0; i < 1024 * 1024; i++) {
            storage.putKey(String.valueOf(i));

            if (i % 1024 == 0) {
                log.info("{}", storage.getDistribution());
            }
        }

        log.info("{}", storage.getDistribution());
    }
}
