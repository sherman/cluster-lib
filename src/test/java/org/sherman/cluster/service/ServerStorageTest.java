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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class ServerStorageTest {
    private static final Logger log = LoggerFactory.getLogger(ServerStorageTest.class);

    @Test
    public void getRandom() {
        ServerStorage serverStorage = new ServerStorageImpl(
            ImmutableList.of(new ServerNode("1", "192.168.5.1"), new ServerNode("2", "192.168.5.2"), new ServerNode("3", "192.168.5.3"))
        );

        for (int i = 0; i < 100; i++) {
            ServerNode node = serverStorage.getRandom();
            log.info("{}", node);
        }
    }
}
