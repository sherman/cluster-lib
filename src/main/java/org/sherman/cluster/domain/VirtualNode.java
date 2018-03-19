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

import com.google.common.base.MoreObjects;
import com.google.common.collect.Range;

public class VirtualNode {
    private final String id;
    private final Range<Long> range;

    public VirtualNode(String id, Range<Long> range) {
        this.id = id;
        this.range = range;
    }

    public String getId() {
        return id;
    }

    public Range<Long> getRange() {
        return range;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VirtualNode that = (VirtualNode) o;

        return com.google.common.base.Objects.equal(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(id);
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("range", range)
            .toString();
    }
}
