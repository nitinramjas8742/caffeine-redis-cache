package com.hybridcache.runtime.hibernate;

public class DistributedCacheLockWrapper {
    private final Object lock;

    public DistributedCacheLockWrapper(Object lock) {
        this.lock = lock;
    }

    public Object getLock() {
        return lock;
    }
}