package io.github.vipcxj.redis.lock;

import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.time.Duration;
import java.util.Date;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class BaseReactiveLock {
    private final Stack<Date> lockedAt;
    private int lockedDeep;
    private boolean enabled;

    public BaseReactiveLock() {
        this.lockedAt = new Stack<>();
        this.lockedDeep = 0;
        this.enabled = true;
    }

    public abstract Mono<Boolean> obtainLock(int level);
    public abstract Mono<Void> releaseLock(int level);
    public abstract boolean isInvalid();
    public abstract Throwable getError();
    public abstract void markInvalid(Throwable t);
    public abstract void makeSureInvalid();
    public abstract void startRenew();
    public abstract void stopRenew();

    public Mono<Boolean> tryLock() {
        return tryLock(1);
    }

    public Mono<Boolean> tryLock(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Invalid lock level: " + level + ", the lock level should be greater than 0.");
        }
        if (!isEnabled()) {
            return Mono.just(false);
        }
        if (isInvalid()) {
            return Mono.error(new IllegalStateException("The lock is invalid.", getError()));
        } else {
            return obtainLock(level)
                    .doOnSuccess(r -> onLock(r, level))
                    .doOnError(this::markInvalid);
        }
    }

    public Mono<Boolean> tryLock(long time, TimeUnit unit) {
        return tryLock(1, time, unit);
    }

    public Mono<Boolean> tryLock(int level, long time, TimeUnit unit) {
        if (level <= 0) {
            throw new IllegalArgumentException("Invalid lock level: " + level + ", the lock level should be greater than 0.");
        }
        if (!isEnabled()) {
            return Mono.just(false);
        }
        if (isInvalid()) {
            return Mono.error(new IllegalStateException("The lock is invalid.", getError()));
        } else {
            Repeat<Object> repeat = Repeat.onlyIf(ctx -> true)
                    .fixedBackoff(Duration.ofMillis(100));
            if (time >= 0) {
                repeat = repeat.timeout(Duration.ofMillis(unit.toMillis(time)));
            }
            return obtainLock(level)
                    .filter(Boolean.TRUE::equals)
                    .repeatWhenEmpty(repeat)
                    .defaultIfEmpty(false)
                    .doOnSuccess(r -> onLock(r, level))
                    .doOnError(this::markInvalid);
        }
    }

    public Mono<Boolean> lock() {
        return lock(1);
    }

    public Mono<Boolean> lock(int level) {
        return tryLock(level, -1, null);
    }

    public Mono<Void> unlock() {
        return unlock(1);
    }

    public Mono<Void> unlock(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Invalid lock level: " + level + ", the lock level should be greater than 0.");
        }
        return Mono.defer(() -> Mono.just(isEnabled()))
                .filter(e -> e)
                .repeatWhenEmpty(Repeat.onlyIf(ctx -> true).fixedBackoff(Duration.ofMillis(10)))
                .then(
                        releaseLock(level)
                                .doOnSuccess(ignored -> this.onUnlock(level))
                );
    }

    public <T> Mono<T> wrap(Supplier<Mono<T>> publisher) {
        return lock().flatMap(ignored ->
                publisher.get().flatMap(v -> unlock().thenReturn(v)).onErrorResume(t -> unlock().then(Mono.error(t)))
        );
    }

    synchronized void onLock(boolean ret, int level) {
        if (isInvalid()) {
            makeSureInvalid();
        }
        if (ret) {
            Date now = new Date();
            long oldDeep = lockedDeep;
            lockedDeep += level;
            for (int i = 0; i < level; ++i) {
                lockedAt.push(now);
            }
            if (oldDeep == 0 && lockedDeep > 0) {
                startRenew();
            }
        }
    }

    synchronized void onUnlock(int level) {
        if (lockedDeep > 0) {
            long oldDeep = lockedDeep;
            lockedDeep -= level;
            for (int i = 0; i < level; ++i) {
                lockedAt.pop();
            }
            if (lockedDeep <= 0) {
                stopRenew();
            }
        }
    }

    public Stack<Date> getLockedAt() {
        return lockedAt;
    }

    public int getLockedDeep() {
        return lockedDeep;
    }

    public synchronized boolean isLocked() {
        return lockedDeep > 0;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
