package io.github.vipcxj.redis.lock;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

public class ReactiveRedisReadWriteLock {

    private final ReactiveRedisReadWriteLockRegistry registry;
    private final ReactiveRedisReadLock readLock;
    private final ReactiveRedisWriteLock writeLock;
    private final String name;
    private final String clientId;
    private final String readLockKey;
    private final String writeLockKey;
    private final String reentrantWriteLockKey;
    private long expire;
    private boolean valid;
    private Throwable error;

    public ReactiveRedisReadWriteLock(ReactiveRedisReadWriteLockRegistry registry, String name) {
        this.registry = registry;
        this.name = name;
        this.clientId = UUID.randomUUID().toString();
        this.readLockKey = registry.getReadLockKey(name);
        this.writeLockKey = registry.getWriteLockKey(name);
        this.reentrantWriteLockKey = registry.getReentrantWriteLockKey(name);
        this.expire = 30 * 1000;
        this.readLock = new ReactiveRedisReadLock(registry, this);
        this.writeLock = new ReactiveRedisWriteLock(registry, this);
        this.valid = true;
    }

    public ReactiveRedisReadLock readLock() {
        return readLock;
    }

    public ReactiveRedisWriteLock writeLock() {
        return writeLock;
    }

    public String getName() {
        return name;
    }

    public String getClientId() {
        return clientId;
    }

    String getReadLockKey() {
        return readLockKey;
    }

    String getWriteLockKey() {
        return writeLockKey;
    }

    String getReentrantWriteLockKey() {
        return reentrantWriteLockKey;
    }

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

    public ReactiveRedisReadWriteLockRegistry getRegistry() {
        return registry;
    }

    public synchronized boolean isInvalid() {
        return !valid;
    }

    public synchronized void markInvalid(Throwable t) {
        this.valid = false;
        this.error = t;
        this.cleanLock();
    }

    public synchronized void makeSureInvalid() {
        if (!this.valid) {
            this.cleanLock();
        }
    }

    private void cleanLock() {
        this.registry.getRedisTemplate()
                .execute(
                        this.registry.getCleanScript(),
                        Arrays.asList(
                                getWriteLockKey(),
                                getReadLockKey(),
                                getReentrantWriteLockKey()
                        ),
                        Collections.singletonList(getClientId())
                ).subscribe();
    }

    public synchronized Throwable getError() {
        return error;
    }
}
