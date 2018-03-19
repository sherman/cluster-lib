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
 *
 * Copied from HIVE project
 */
public class Murmur3 {
    // from 64-bit linear congruential generator
    public static final long NULL_HASHCODE = 2862933555777941757L;

    // Constants for 128 bit variant
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;

    public static final int DEFAULT_SEED = 104729;

    private Murmur3() {
    }

    public static long hash64(byte[] data) {
        return hash64(data, 0, data.length, DEFAULT_SEED);
    }

    public static long hash64(byte[] data, int offset, int length) {
        return hash64(data, offset, length, DEFAULT_SEED);
    }

    /**
     * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
     *
     * @param data   - input byte array
     * @param length - length of array
     * @param seed   - seed. (default is 0)
     * @return - hashcode
     */
    public static long hash64(byte[] data, int offset, int length, int seed) {
        long hash = seed;
        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int i8 = i << 3;
            long k = ((long) data[offset + i8] & 0xff)
                | (((long) data[offset + i8 + 1] & 0xff) << 8)
                | (((long) data[offset + i8 + 2] & 0xff) << 16)
                | (((long) data[offset + i8 + 3] & 0xff) << 24)
                | (((long) data[offset + i8 + 4] & 0xff) << 32)
                | (((long) data[offset + i8 + 5] & 0xff) << 40)
                | (((long) data[offset + i8 + 6] & 0xff) << 48)
                | (((long) data[offset + i8 + 7] & 0xff) << 56);

            // mix functions
            k *= C1;
            k = Long.rotateLeft(k, R1);
            k *= C2;
            hash ^= k;
            hash = Long.rotateLeft(hash, R2) * M + N1;
        }

        // tail
        long k1 = 0;
        int tailStart = nblocks << 3;
        switch (length - tailStart) {
            case 7:
                k1 ^= ((long) data[offset + tailStart + 6] & 0xff) << 48;
            case 6:
                k1 ^= ((long) data[offset + tailStart + 5] & 0xff) << 40;
            case 5:
                k1 ^= ((long) data[offset + tailStart + 4] & 0xff) << 32;
            case 4:
                k1 ^= ((long) data[offset + tailStart + 3] & 0xff) << 24;
            case 3:
                k1 ^= ((long) data[offset + tailStart + 2] & 0xff) << 16;
            case 2:
                k1 ^= ((long) data[offset + tailStart + 1] & 0xff) << 8;
            case 1:
                k1 ^= ((long) data[offset + tailStart] & 0xff);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, R1);
                k1 *= C2;
                hash ^= k1;
        }

        // finalization
        hash ^= length;
        hash = fmix64(hash);

        return hash;
    }

    private static long fmix64(long h) {
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }

}
