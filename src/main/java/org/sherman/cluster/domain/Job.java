package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class Job {
    private final String id;
    private final int cpus;

    public Job(String id, int cpus) {
        this.id = id;
        this.cpus = cpus;
    }

    public String getId() {
        return id;
    }

    public int getCpus() {
        return cpus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Job job = (Job) o;
        return cpus == job.cpus && Objects.equal(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, cpus);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("cpus", cpus)
            .toString();
    }
}
