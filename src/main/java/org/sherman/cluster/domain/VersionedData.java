package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * This class helps to resolve a problem with the lamport clock approach. {@code sessionId} is a tiebreaker in case when two events are not ordered (were concurrent).
 */
public class VersionedData {
    private final int sessionId;
    private final long ts;
    private final String data;

    public VersionedData(int sessionId, long ts, String data) {
        this.sessionId = sessionId;
        this.ts = ts;
        this.data = data;
    }

    public int getSessionId() {
        return sessionId;
    }

    public long getTs() {
        return ts;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VersionedData that = (VersionedData) o;
        return sessionId == that.sessionId && ts == that.ts && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, ts, data);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("sessionId", sessionId)
            .add("ts", ts)
            .add("data", data)
            .toString();
    }
}
