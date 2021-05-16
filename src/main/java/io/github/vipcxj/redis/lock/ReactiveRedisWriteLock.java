package io.github.vipcxj.redis.lock;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.retry.Repeat;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;

public class ReactiveRedisWriteLock extends BaseReactiveLock {

    private final ReactiveRedisReadWriteLockRegistry registry;
    private final ReactiveRedisReadWriteLock parent;
    private Disposable renewTask;

    ReactiveRedisWriteLock(ReactiveRedisReadWriteLockRegistry registry, ReactiveRedisReadWriteLock parent) {
        super();
        this.registry = registry;
        this.parent = parent;
    }

/*    private Mono<String> log(int level) {
        return Mono.just("").doOnSuccess(ignored -> {
            String now = new SimpleDateFormat("hh:mm:ss.SSS").format(new Date());
            System.out.println("[" + now + "] Trying to write lock " + parent.getClientId() + " with level " + level + ".");
        });
    }*/

    @Override
    public Mono<Boolean> obtainLock(int level) {
        return /*log(level).then(*/Mono.from(registry.getRedisTemplate().execute(
                registry.getWriteLockScript(),
                Arrays.asList(
                        parent.getWriteLockKey(),
                        parent.getReadLockKey(),
                        parent.getReentrantWriteLockKey()
                ),
                Arrays.asList(
                        parent.getClientId(),
                        Long.toString(parent.getExpire()),
                        Integer.toString(level)
                )))
                .map(r -> r > 0)
/*                .doOnSuccess(locked -> {
                    if (locked) {
                        System.out.println("The write lock " + parent.getClientId() + " is locked.");
                    }
                }))*/;
    }

    @Override
    public Mono<Void> releaseLock(int level) {
        return Mono.from(registry.getRedisTemplate().execute(
                registry.getWriteUnlockScript(),
                Arrays.asList(
                        parent.getWriteLockKey(),
                        parent.getReentrantWriteLockKey()
                ),
                Arrays.asList(
                        parent.getClientId(),
                        Long.toString(parent.getExpire()),
                        Integer.toString(level)
                )))
                .then();
    }

    @Override
    public synchronized void startRenew() {
        if (renewTask == null) {
            renewTask = registry.getRedisTemplate().expire(parent.getWriteLockKey(), Duration.ofMillis(parent.getExpire()))
                    .then(registry.getRedisTemplate().expire(parent.getReentrantWriteLockKey(), Duration.ofMillis(parent.getExpire())))
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
}
