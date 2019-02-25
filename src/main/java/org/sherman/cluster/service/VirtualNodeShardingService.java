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

import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.Ring;
import org.sherman.cluster.domain.ServerNode;
import org.sherman.cluster.domain.VirtualNode;
import org.sherman.cluster.util.RoundRobinIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VirtualNodeShardingService implements HashShardingService {
    private final Ring ring;
    private final ServerStorage serverStorage;

    private final Map<VirtualNode, ServerNode> virtualNodesToServerNodes = new HashMap<>();

    public VirtualNodeShardingService(ServerStorage serverStorage, Ring ring) {
        this.serverStorage = serverStorage;
        this.ring = ring;

        init();
    }

    private void init() {
        List<ServerNode> servers = serverStorage.getServers();

        RoundRobinIterator roundRobinIterator = new RoundRobinIterator(servers.size());

        virtualNodesToServerNodes.putAll(
            ring.getVirtualNodes().stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        virtualNode -> servers.get(roundRobinIterator.next() - 1)
                    )
                )
        );
    }

    @NotNull
    @Override
    public ServerNode getNodeByKey(@NotNull String key) {
        return getNodeByKey(key.getBytes());
    }

    @NotNull
    @Override
    public ServerNode getNodeByKey(@NotNull byte[] key) {
        VirtualNode node = ring.getByKey(key);
        return virtualNodesToServerNodes.get(node);
    }
}
