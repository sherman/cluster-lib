package org.sherman.cluster.domain;

public class LamportClock {
    private long timestamp;

    public LamportClock(long timestamp) {
        this.timestamp = timestamp;
    }

    public long tick(long newTime) {
        timestamp = Math.max(timestamp, newTime);
        timestamp++;
        return timestamp;
    }

    public long get() {
        return timestamp;
    }

    public long inc() {
        return ++timestamp;
    }

    public void restart() {
        timestamp = 1;
    }
}
