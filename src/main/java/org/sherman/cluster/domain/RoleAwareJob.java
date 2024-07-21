package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class RoleAwareJob {
    private final Role role;
    private final Job job;

    public RoleAwareJob(Role role, Job job) {
        this.role = role;
        this.job = job;
    }

    public Role getRole() {
        return role;
    }

    public Job getJob() {
        return job;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoleAwareJob that = (RoleAwareJob) o;
        return role == that.role && Objects.equal(job, that.job);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(role, job);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("role", role)
            .add("job", job)
            .toString();
    }
}
