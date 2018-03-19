package org.sherman.cluster.util;

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

public class Integers {
    private Integers() {
    }

    /**
     * Value must be in range (1, 1073741824) and is to be a power of two.
     */
    public static boolean isPowerOfTwo(int value) {
        return (
            value == 1 || value == 2 || value == 4 || value == 8 || value == 16 || value == 32 ||
                value == 64 || value == 128 || value == 256 || value == 512 || value == 1024 ||
                value == 2048 || value == 4096 || value == 8192 || value == 16384 ||
                value == 32768 || value == 65536 || value == 131072 || value == 262144 ||
                value == 524288 || value == 1048576 || value == 2097152 ||
                value == 4194304 || value == 8388608 || value == 16777216 ||
                value == 33554432 || value == 67108864 || value == 134217728 ||
                value == 268435456 || value == 536870912 || value == 1073741824);
    }

}
