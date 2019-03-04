package org.sherman.cluster.service;

import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;

import java.util.Map;

/**
 * @author Denis M. Gabaydulin
 * @since 04.03.19
 */
public interface HbaseStyleStorage<T extends Comparable<T>> {
    void putKey(@NotNull T key);

    @NotNull
    ServerNode getServerNodeByKey(@NotNull T key);

    Map<ServerNode, Integer> getDistribution();
}
