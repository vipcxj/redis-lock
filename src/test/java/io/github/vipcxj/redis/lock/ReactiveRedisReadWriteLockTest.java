package io.github.vipcxj.redis.lock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Schedulers;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

@SpringBootTest(classes = RedisTestConfiguration.class)
public class ReactiveRedisReadWriteLockTest {

    @Autowired
    private ReactiveRedisTemplate<String, Integer> template;
    @Autowired
    private ReactiveRedisReadWriteLockRegistry lockRegistry;
    private Random random;

    @BeforeAll
    public static void setUp() {
        Hooks.onOperatorDebug();
    }

    @BeforeEach
    public void prepareData() {
        this.random = new Random();
        template.opsForValue().set("a", 0).block();
    }

    @Test
    public void test() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; ++i) {
            int finalI = i;
            Thread thread = new Thread(() -> lockRegistry.forRead("a", template.opsForValue().get("a").doOnSuccess(a -> log(finalI, "Got a: " + a))/*.delayElement(duration())*/.flatMap(a ->
                    lockRegistry.forUpgrade("a", template.opsForValue().get("a").flatMap(a1 -> template.opsForValue().set("a", a1 + 1).doOnSuccess(r -> log(finalI, "Plus a with 1"))))/*.delayElement(duration())*/.then(
                            template.opsForValue().get("a").doOnSuccess(a1 -> log(finalI, "Got a: " + a1)).delayElement(duration()).flatMap(a1 ->
                                    lockRegistry.forUpgrade("a", template.opsForValue().get("a").flatMap(a2 -> template.opsForValue().set("a", a2 - 1).doOnSuccess(r -> log(finalI, "Minus a with 1"))))
                            )
                    )
            )).block());
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        lockRegistry.forRead("a", template.opsForValue().get("a").doOnSuccess(a -> Assertions.assertEquals(0, a))).block();
        System.out.println("Time used: " + (System.currentTimeMillis() - start) / 1000 + "s");
    }

    private void log(int i, String message) {
        System.out.println("[Task " + i + "][" + new SimpleDateFormat("hh:mm:ss.SSS").format(new Date()) + "] Thread " + Thread.currentThread().getId() + ": " + message + ".");
    }

    private Duration duration() {
        return Duration.ofMillis(random.nextInt(1000));
    }
}
