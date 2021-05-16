package io.github.vipcxj.redis.lock;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

public class ReactiveRedisReadLock extends BaseReactiveLock {

    private final ReactiveRedisReadWriteLockRegistry registry;
    private final ReactiveRedisReadWriteLock parent;
    private Disposable renewTask;

    ReactiveRedisReadLock(ReactiveRedisReadWriteLockRegistry registry, ReactiveRedisReadWriteLock parent) {
        super();
        this.registry = registry;
        this.parent = parent;
    }

/*    private Mono<String> log(int level) {
        return Mono.just("").doOnSuccess(ignored -> {
            String now = new SimpleDateFormat("hh:mm:ss.SSS").format(new Date());
            System.out.println("[" + now + "] Trying to read lock " + parent.getClientId() + " with level " + level + ".");
        });
    }*/

    @Override
    public Mono<Boolean> obtainLock(int level) {
        return /*log(level).then(*/Mono.from(registry.getRedisTemplate().execute(
                registry.getReadLockScript(),
                Arrays.asList(
                        parent.getWriteLockKey(),
                        parent.getReadLockKey(),
                        parent.getClientId()
                ),
                Arrays.asList(
                        Long.toString(parent.getExpire()),
                        Integer.toString(level)
                )))
                .map(r -> r > 0)
/*                .doOnSuccess(locked -> {
                    if (locked) {
                        System.out.println("The read lock " + parent.getClientId() + " is locked.");
                    }
                }))*/;
    }

    @Override
    public Mono<Void> releaseLock(int level) {
        return Mono.from(registry.getRedisTemplate().execute(
                registry.getReadUnlockScript(),
                Arrays.asList(
                        parent.getReadLockKey(),
                        parent.getClientId()
                ),
                Collections.singletonList(Integer.toString(level))))
/*                .doOnSuccess(r -> {
                    if (r >= 0) {
                        System.out.println("The read lock " + parent.getClientId() + " is unlocked.");
                    }
                })*/
                .then();
    }

    @Override
    public synchronized void startRenew() {
        if (renewTask == null) {
            renewTask = registry.getRedisTemplate()
                    .expire(parent.getReadLockKey(), Duration.ofMillis(parent.getExpire()))
                    .repeatWhen(Repeat.onlyIf(ctx -> true).fixedBackoff(Duration.ofMillis(parent.getExpire() / 2)))
                    .subscribe();
        }
    }

    @Override
    public synchronized void stopRenew() {
        if (renewTask != null) {
            renewTask.dispose();
            renewTask = null;
        }
    }

    @Override
    public boolean isInvalid() {
        return parent.isInvalid();
    }

    @Override
    public Throwable getError() {
        return parent.getError();
    }

    @Override
    public void markInvalid(Throwable t) {
        parent.markInvalid(t);
    }

    @Override
    public void makeSureInvalid() {
        parent.makeSureInvalid();
    }

    public <T> Mono<T> upgrade(Supplier<Mono<T>> supplier) {
        int lockedDeep = getLockedDeep();
        return unlock(lockedDeep).then(
                parent.writeLock().wrap(supplier)
                .flatMap(v -> lock(lockedDeep).thenReturn(v).onErrorResume(t -> lock(lockedDeep).then(Mono.error(t))))
        );
    }
}
