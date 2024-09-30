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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.sherman.cluster.domain.Ring;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class VirtualNodeShardingServiceTest {
    private static final Logger log = LoggerFactory.getLogger(VirtualNodeShardingServiceTest.class);

    @Test
    public void getByKey() {
        var serverStorage = new ServerStorageImpl(
            List.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        var ring = new Ring(128); // must be enough buckets to spread a data evenly

        var shardingService = new VirtualNodeShardingService(serverStorage, ring);

        var distribution = new HashMap<ServerNode, AtomicInteger>();
        for (var i = 0; i < 1024 * 1024; i++) {
            var serverNode = shardingService.getNodeByKey(String.valueOf(i));
            distribution.putIfAbsent(serverNode, new AtomicInteger());
            distribution.get(serverNode).incrementAndGet();
        }

        log.info("{}", distribution);
    }
}
