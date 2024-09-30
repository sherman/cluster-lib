package org.sherman.cluster.service;

/*
 * Copyright (C) 2019 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.Range;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class LongRangedShardingServiceTest {
    private static final Logger log = LoggerFactory.getLogger(LongRangedShardingServiceTest.class);

    @Test
    public void getByKeyBadExample() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var shardingService = new LongRangedShardingService(serverStorage, Range.closed(0L, Long.MAX_VALUE));

        var distribution = new HashMap<ServerNode, AtomicInteger>();
        for (var i = 0; i < 1024 * 1024; i++) {
            var serverNode = shardingService.getNodeByKey((long) i);
            distribution.putIfAbsent(serverNode, new AtomicInteger());
            distribution.get(serverNode).incrementAndGet();
        }

        log.info("{}", distribution); // all keys are mapped to a single server :-(
    }

    @Test
    public void getByKeyGoodExample() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var shardingService = new LongRangedShardingService(serverStorage, Range.closed(0L, Long.MAX_VALUE));

        var random = ThreadLocalRandom.current();

        var distribution = new HashMap<ServerNode, AtomicInteger>();
        for (var i = 0; i < 1024 * 1024; i++) {
            var serverNode = shardingService.getNodeByKey(random.nextLong(0, Long.MAX_VALUE));
            distribution.putIfAbsent(serverNode, new AtomicInteger());
            distribution.get(serverNode).incrementAndGet();
        }

        log.info("{}", distribution); // all keys are mapped to a single server :-(
    }
}
