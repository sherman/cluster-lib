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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LongRangedShardingServiceTest {
    private static final Logger log = LoggerFactory.getLogger(LongRangedShardingServiceTest.class);

    @Test
    public void getByKey() {
        ServerStorage serverStorage = new ServerStorageImpl(
            ImmutableList.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        RangedShardingService shardingService = new LongRangedShardingService(serverStorage, Range.closed(0L, Long.MAX_VALUE));

        Map<ServerNode, AtomicInteger> distribution = new HashMap<>();
        for (int i = 0; i < 1024 * 1024; i++) {
            ServerNode serverNode = shardingService.getNodeByKey((long) i);
            distribution.putIfAbsent(serverNode, new AtomicInteger());
            distribution.get(serverNode).incrementAndGet();
        }

        log.info("{}", distribution); // all keys are mapped to a single server :-(
    }
}
