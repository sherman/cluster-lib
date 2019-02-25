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
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongRangedShardingService implements RangedShardingService<Long> {
    private static final Logger log = LoggerFactory.getLogger(LongRangedShardingService.class);

    private final Range<Long> closedRange;
    private final ServerStorage serverStorage;
    private final RangeMap<Long, ServerNode> rangesToServerNodes = TreeRangeMap.create();

    public LongRangedShardingService(ServerStorage serverStorage, Range<Long> closedRange) {
        this.closedRange = closedRange;
        this.serverStorage = serverStorage;

        this.init();
    }

    private void init() {
        Long bound = closedRange.upperEndpoint() / (serverStorage.getServers().size() / 2);

        log.info("Size: {}", bound);

        long init = closedRange.lowerEndpoint();

        for (int i = 0; i < serverStorage.getServers().size(); i++) {
            log.info("{} {}", init, init + bound);
            ServerNode serverNode = serverStorage.getServers().get(i);
            Range<Long> range = Range.closed(init, init + bound);
            rangesToServerNodes.put(range, serverNode);

            init = init + bound + 1;
        }
    }

    @NotNull
    @Override
    public ServerNode getNodeByKey(@NotNull Long key) {
        return rangesToServerNodes.get(key);
    }
}
