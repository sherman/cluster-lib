package org.sherman.cluster.service;

public interface DistributedCacheService {
    /**
     * {@param partition} equals to -1, cache all
     */
    void cache(int serverIs, String data, int partition);

    float getPercentOfUnique();
}
