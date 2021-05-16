package io.github.vipcxj.redis.lock;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReactiveRedisReadWriteLockRegistry {

    private final ReactiveStringRedisTemplate redisTemplate;
    private final RedisScript<Long> readLockScript;
    private final RedisScript<Long> readUnlockScript;
    private final RedisScript<Long> writeLockScript;
    private final RedisScript<Long> writeUnlockScript;
    private final RedisScript<Void> cleanScript;
    private final String readLockPrefix;
    private final String writeLockPrefix;
    private final String reentrantWriteLockPrefix;
    private final long expire;
    private final Map<String, Map<String, Integer>> readLockMap;
    private final Map<String, String> writeLockMap;
    private final Map<String, Integer> reentrantWriteLockMap;

    public ReactiveRedisReadWriteLockRegistry(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.readLockPrefix = "cxj_read_lock_";
        this.writeLockPrefix = "cxj_write_lock_";
        this.reentrantWriteLockPrefix = "cxj_reentrant_write_lock_";
        this.readLockMap = new HashMap<>();
        this.writeLockMap = new HashMap<>();
        this.reentrantWriteLockMap = new HashMap<>();
        this.expire = 20 * 1000;
        // KEYS: write_lock_key, read_lock_key, client_id
        // ARGS: expire_time, lock_level.
        String SCRIPT_READ_LOCK = "" +
                "local w = redis.call('GET', KEYS[1]);" +
                "if not w or w == KEYS[3] then" +
                "   local count = redis.call('HINCRBY', KEYS[2], KEYS[3], ARGV[2]);" +
                "   local t = redis.call('PTTL', KEYS[2]);" +
                "   redis.call('PEXPIRE', KEYS[2], math.max(t, ARGV[1]));" +
                "   return count;" +
                "else " +
                "   return 0;" +
                "end;";
        this.readLockScript = new DefaultRedisScript<>(SCRIPT_READ_LOCK, Long.class);
        // KEYS: read_lock_key, client_id
        // ARGS: lock_level
        String SCRIPT_READ_UNLOCK = "" +
                "local count = redis.call('HGET', KEYS[1], KEYS[2]);" +
                "if count then " +
                "   if (tonumber(count) > tonumber(ARGV[1])) then " +
                "       count = tonumber(count) - tonumber(ARGV[1]);" +
                "       redis.call('HSET', KEYS[1], KEYS[2], count);" +
                "       return count;" +
                "   else " +
                "       redis.call('HDEL', KEYS[1], KEYS[2]);" +
                "       return 0;" +
                "   end;" +
                "else " +
                "   return -1;" +
                "end;";
        this.readUnlockScript = new DefaultRedisScript<>(SCRIPT_READ_UNLOCK, Long.class);
        // KEYS: write_lock_key, read_lock_key, reentrant_write_lock_key
        // ARGS: client_id, expire_time, lock_level.
        String SCRIPT_WRITE_LOCK = "" +
                "local rn = redis.call('HLEN', KEYS[2]);" +
                "if rn == 0 or (rn == 1 and redis.call('HEXISTS', KEYS[2], ARGV[1])) then " +
                "   if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then " +
                "       redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[2]);" +
                "       return tonumber(ARGV[3]);" +
                "   elseif redis.call('GET', KEYS[1]) == ARGV[1] then " +
                "       local count = redis.call('INCRBY', KEYS[3], ARGV[3]);" +
                "       redis.call('PEXPIRE', KEYS[1], ARGV[2]);" +
                "       redis.call('PEXPIRE', KEYS[3], ARGV[2]);" +
                "       return count;" +
                "   else " +
                "       return 0;" +
                "   end;" +
                "else " +
                "   return 0;" +
                "end;";
        this.writeLockScript = new DefaultRedisScript<>(SCRIPT_WRITE_LOCK, Long.class);
        // KEYS: write_lock_key, reentrant_write_lock_key
        // ARGS: client_id, expire_time, lock_level
        String SCRIPT_WRITE_UNLOCK = "" +
                "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                "   local count = redis.call('GET',KEYS[2]);" +
                "   if count then " +
                "       if (tonumber(count) > tonumber(ARGV[3])) then " +
                "           count = tonumber(count) - tonumber(ARGV[3]);" +
                "           redis.call('SET', KEYS[2], count, 'PX', ARGV[2]);" +
                "           return count;" +
                "       else " +
                "           redis.call('DEL', KEYS[1]);" +
                "           redis.call('DEL', KEYS[2]);" +
                "           return 0;" +
                "       end;" +
                "   else " +
                "       redis.call('DEL',KEYS[1]);" +
                "       return 0;" +
                "   end;" +
                "else " +
                "   return -1;" +
                "end;";
        this.writeUnlockScript = new DefaultRedisScript<>(SCRIPT_WRITE_UNLOCK, Long.class);
        // KEYS: write_lock_key, read_lock_key, reentrant_write_lock_key
        // ARGS: client_id.
        String SCRIPT_CLEAN = "" +
                "redis.call('HDEL', KEYS[2], ARGV[1]);" +
                "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                "   redis.call('DEL', KEYS[1]);" +
                "   redis.call('DEL', KEYS[3]);" +
                "end;";
        this.cleanScript = new DefaultRedisScript<>(SCRIPT_CLEAN, Void.class);
    }

    public ReactiveStringRedisTemplate getRedisTemplate() {
        return redisTemplate;
    }

    public RedisScript<Long> getReadLockScript() {
        return readLockScript;
    }

    public RedisScript<Long> getReadUnlockScript() {
        return readUnlockScript;
    }

    public RedisScript<Long> getWriteLockScript() {
        return writeLockScript;
    }

    public RedisScript<Long> getWriteUnlockScript() {
        return writeUnlockScript;
    }

    public RedisScript<Void> getCleanScript() {
        return cleanScript;
    }

    // 构建锁的key
    public String getReadLockKey(String name){
        return readLockPrefix + name;
    }

    public String getWriteLockKey(String name){
        return writeLockPrefix + name;
    }

    public String getReentrantWriteLockKey(String name){
        return reentrantWriteLockPrefix + name;
    }

    public long getExpire() {
        return expire;
    }

    public ReactiveRedisReadWriteLock createLock(String name) {
        return new ReactiveRedisReadWriteLock(this, name);
    }

    public <T> Mono<T> wrapMono(String key, Function<ReactiveRedisReadWriteLock, Mono<T>> publisherFactory) {
        CtxLockKey ctx_lock_key = new CtxLockKey(key);
        return Mono.subscriberContext().map(ctx -> ctx.<ReactiveRedisReadWriteLock>get(ctx_lock_key))
                .flatMap(publisherFactory)
                .subscriberContext(ctx -> {
                    if (ctx.hasKey(ctx_lock_key)) {
                        return ctx;
                    } else {
                        return ctx.put(ctx_lock_key, createLock(key));
                    }
                });
    }

    public <T> Flux<T> wrapFlux(String key, Function<ReactiveRedisReadWriteLock, Flux<T>> publisherFactory) {
        CtxLockKey ctx_lock_key = new CtxLockKey(key);
        return Mono.subscriberContext().map(ctx -> ctx.<ReactiveRedisReadWriteLock>get(ctx_lock_key))
                .flatMapMany(publisherFactory)
                .subscriberContext(ctx -> {
                    if (ctx.hasKey(ctx_lock_key)) {
                        return ctx;
                    } else {
                        return ctx.put(ctx_lock_key, createLock(key));
                    }
                });
    }

    public <T> Mono<T> forRead(String key, Mono<T> mono) {
        return wrapMono(key, lock -> lock.readLock().wrap(() -> mono));
    }

    public <T> Mono<T> forWrite(String key, Mono<T> mono) {
        return wrapMono(key, lock -> lock.writeLock().wrap(() -> mono));
    }

    public <T> Mono<T> forUpgrade(String key, Mono<T> mono) {
        return wrapMono(key, lock -> lock.readLock().upgrade(() -> mono));
    }

    public <T> Mono<T> getValue(String key, Function<String, Mono<T>> supplier, ReactiveRedisTemplate<String, T> template, Duration duration) {
        return wrapMono(key, lock -> lock.readLock().wrap(() -> template.opsForValue().get(key).switchIfEmpty(
                lock.writeLock().wrap(
                        () -> supplier.apply(key).flatMap(
                                v -> template.opsForValue().set(key, v, duration).thenReturn(v).onErrorReturn(v)
                        )
                )
        )));
    }

    public <T> Mono<T> updateValue(String key, T value, BiFunction<String, T, Mono<T>> updater, ReactiveRedisTemplate<String, T> template, Duration duration) {
        return wrapMono(key, lock -> lock.writeLock().wrap(() -> updater.apply(key, value).flatMap(old -> {
            if (duration != null) {
                return template.opsForValue().set(key, value, duration).thenReturn(old).onErrorReturn(old);
            } else {
                return template.opsForValue().set(key, value).thenReturn(old).onErrorReturn(old);
            }
        })));
    }

    public static Mono<ReactiveRedisReadWriteLock> getLock(String key) {
        return Mono.subscriberContext().flatMap(ctx -> {
            ReactiveRedisReadWriteLock lock = ctx.get(new CtxLockKey(key));
            return Mono.justOrEmpty(lock);
        });
    }

    public synchronized int readLock(String key, String clientId, int level) {
        String wv = writeLockMap.get(key);
        if (wv == null || wv.equals(clientId)) {
            return readLockMap.computeIfAbsent(key, k -> new HashMap<>()).compute(clientId, (k, v) -> v != null ? v + level : level);
        } else {
            return 0;
        }
    }

    public synchronized int readUnlock(String key, String clientId, int level) {
        Map<String, Integer> lockMap = readLockMap.get(key);
        if (lockMap == null) {
            return -1;
        }
        Integer lockedLevel = lockMap.get(clientId);
        if (lockedLevel == null) {
            return -1;
        }
        int newLevel = Math.max(0, lockedLevel - level);
        if (newLevel > 0) {
            lockMap.put(clientId, newLevel);
        } else {
            lockMap.remove(clientId);
        }
        if (lockMap.isEmpty()) {
            readLockMap.remove(key);
        }
        return newLevel;
    }

    public synchronized int writeLock(String key, String clientId, int level) {
        if (readLockMap.get(key) == null || readLockMap.get(key).isEmpty() || (readLockMap.get(key).size() == 1 && readLockMap.get(key).containsKey(clientId))) {
            writeLockMap.put(key, clientId);
            return reentrantWriteLockMap.compute(key, (k, v) -> v != null ? v + level : level);
        } else {
            return 0;
        }
    }

    public synchronized int writeUnlock(String key, String clientId, int level) {
        String v = writeLockMap.get(key);
        if (v != null && v.equals(clientId)) {
            Integer r = reentrantWriteLockMap.get(key);
            int newLevel = Math.max(0, r - level);
            if (newLevel > 0) {
                reentrantWriteLockMap.put(key, newLevel);
            } else {
                reentrantWriteLockMap.remove(key);
                writeLockMap.remove(key);
            }
            return newLevel;
        } else {
            return -1;
        }
    }
}
