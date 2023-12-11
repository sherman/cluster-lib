package org.sherman.cluster.service;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class DistributedCacheServiceImpl implements DistributedCacheService {
    private final Map<Integer, LruCache> cache = new HashMap<>();
    private final HashFunction function = Hashing.murmur3_128();
    private final int maxPartitions;

    public DistributedCacheServiceImpl(int maxServers, int maxPartitions) {
        this.maxPartitions = maxPartitions;
        for (int server = 1; server <= maxServers; server++) {
            cache.put(server, new LruCache(100));
        }
    }

    @Override
    public void cache(int serverId, String data, int partition) {
        int hash = getHashKey(data);
        LruCache serverCache = cache.get(serverId);
        if (partition == -1 || hash % maxPartitions == partition) {
            serverCache.put(hash, data);
        }
    }

    @Override
    public float getPercentOfUnique() {
        Set<Integer> uniqueItems = new HashSet<>();
        float total = 0;
        for (Map.Entry<Integer, LruCache> entry : cache.entrySet()) {
            uniqueItems.addAll(entry.getValue().keySet());
            total += entry.getValue().size();
        }
        return (float) ((uniqueItems.size() / total) * 100.0);
    }

    private int getHashKey(String data) {
        return Math.abs(function.newHasher().putString(data, StandardCharsets.UTF_8).hash().asInt());
    }

    private static final class LruCache extends LinkedHashMap<Integer, String> {
        private final int maxSize;

        private LruCache(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size() > this.maxSize;
        }
    }
}
