package org.sherman.cluster.service;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class ResourceSchedulerImpl implements ResourceScheduler {

    private static class Executor extends ThreadPoolExecutor {

        public Executor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                        @NotNull TimeUnit unit,
                        @NotNull BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }
    }
}
