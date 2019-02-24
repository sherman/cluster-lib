package org.sherman.cluster.domain;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.util.Integers;
import org.sherman.cluster.util.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class Ring {
    private static final Logger log = LoggerFactory.getLogger(Ring.class);

    private final int nodes;

    private final RangeMap<Long, VirtualNode> virtualNodes = TreeRangeMap.create();

    public Ring(int nodes) {
        Preconditions.checkArgument(Integers.isPowerOfTwo(nodes), "Value must be in range (1, 1073741824) and is to be a power of two!");

        this.nodes = nodes;

        init();
    }

    public void init() {
        long lower = Long.MIN_VALUE;
        long upper = Long.MAX_VALUE;

        if (nodes == 1) {
            VirtualNode node = getNode(lower, upper);
            virtualNodes.put(node.getRange(), node);
            return;
        }

        long bound = Long.MAX_VALUE / (nodes / 2);

        log.info("Size: {}", bound);

        long init = Long.MIN_VALUE;

        for (int i = 0; i < nodes; i++) {
            log.info("{} {}", init, init + bound);
            VirtualNode node = getNode(init, init + bound);
            virtualNodes.put(node.getRange(), node);

            init = init + bound + 1;
        }

        Preconditions.checkArgument(virtualNodes.asMapOfRanges().keySet().stream().findFirst().get().lowerEndpoint().equals(lower), "Start is invalid!");
        Preconditions.checkArgument(virtualNodes.asDescendingMapOfRanges().keySet().stream().findFirst().get().upperEndpoint().equals(upper), "End is invalid!");
    }

    @NotNull
    public VirtualNode getByToken(long token) {
        return virtualNodes.get(token);
    }

    @NotNull
    public VirtualNode getByKey(@NotNull byte[] key) {
        return getByToken(Tokens.getTokenByKey(key));
    }

    @NotNull
    public VirtualNode getByKey(@NotNull String key) {
        return getByKey(key.getBytes());
    }

    @NotNull
    public VirtualNode getByKey(long key) {
        return getByKey(Longs.toByteArray(key));
    }

    public List<VirtualNode> getVirtualNodes() {
        return ImmutableList.copyOf(virtualNodes.asMapOfRanges().values());
    }

    private VirtualNode getNode(long lower, long upper) {
        Range<Long> range = Range.closed(lower, upper);
        String id = UUID.randomUUID().toString();
        return new VirtualNode(id, range);
    }
}
