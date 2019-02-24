package org.sherman.cluster.service;

/*
 * Copyright (C) 2018 by Denis M. Gabaydulin
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
import org.sherman.cluster.domain.ServerNode;

import java.util.List;
import java.util.Random;

public class ServerStorageImpl implements ServerStorage {
    private final List<ServerNode> nodes;
    private final Random random = new Random();

    public ServerStorageImpl(List<ServerNode> nodes) {
        this.nodes = ImmutableList.copyOf(nodes);
    }

    @Override
    public List<ServerNode> getServers() {
        return nodes;
    }

    @Override
    public ServerNode getRandom() {
        return nodes.toArray(new ServerNode[nodes.size()])[random.nextInt(nodes.size())];
    }
}
