package io.github.vipcxj.redis.lock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.embedded.RedisServer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@SpringBootConfiguration
@EnableAutoConfiguration
public class RedisTestConfiguration {

    private RedisServer redisServer;
    @Value("${spring.redis.port:6379}")
    private int redisPort;

    @PostConstruct
    public void postConstruct() throws IOException {
        this.redisServer = new RedisServer(redisPort);
        this.redisServer.start();
        System.out.println("redis server started on port: " + redisPort + ".");
    }

    @PreDestroy
    public void preDestroy() {
        this.redisServer.stop();
        System.out.println("redis server stopped.");
    }

    @Bean
    public LettuceConnectionFactory lettuceConnectionFactory() {
        return new LettuceConnectionFactory();
    }

    @Bean
    public ReactiveRedisReadWriteLockRegistry reactiveRedisReadWriteLockRegistry(ReactiveStringRedisTemplate redisTemplate) {
        return new ReactiveRedisReadWriteLockRegistry(redisTemplate);
    }

    @Bean
    public ReactiveRedisTemplate<String, Integer> integerReactiveRedisTemplate(LettuceConnectionFactory factory) {
        StringRedisSerializer keySerializer = new StringRedisSerializer();
        RedisSerializationContext.RedisSerializationContextBuilder<String, Integer> builder = RedisSerializationContext.newSerializationContext(keySerializer);
        RedisSerializationContext<String, Integer> context = builder.value(new GenericToStringSerializer<>(Integer.class)).build();
        return new ReactiveRedisTemplate<>(factory, context);
    }
}
