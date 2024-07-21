package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class Jobs implements Comparable<Jobs> {
    private int totalCpus;
    private final Set<Job> jobs;

    public Jobs(int totalCpus, Set<Job> jobs) {
        this.totalCpus = totalCpus;
        this.jobs = jobs;
    }

    public int getTotalCpus() {
        return totalCpus;
    }

    public Set<Job> getJobs() {
        return jobs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Jobs jobs1 = (Jobs) o;
        return totalCpus == jobs1.totalCpus && Objects.equal(jobs, jobs1.jobs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(totalCpus, jobs);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("totalCpus", totalCpus)
            .add("jobs", jobs)
            .toString();
    }

    @Override
    public int compareTo(@NotNull Jobs job) {
        return this.totalCpus - job.totalCpus;
    }

    public void addJob(Job job) {
        totalCpus += job.getCpus();
        jobs.add(job);
    }
}
