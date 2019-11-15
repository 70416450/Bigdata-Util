package com.bigdata.redis.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/15 10:35
 * @describe 配置文件需要加入如下参数（demo）-->使用redis.mode参数控制其使用模式
 * ##redis哨兵集群配置
 * redis.mode=sentinel
 * redis.hosts=10.28.17.113:26379,10.28.17.171:26379,10.28.17.175:26379
 * ##redis集群配置
 * #redis.mode=cluster
 * #redis.hosts=10.28.17.138:6379,10.28.17.223:6379,10.28.17.220:6379
 * ##redis单机配置
 * #redis.mode=standalone
 * #redis.hosts=10.28.17.138
 * ##当调用borrow Object方法时，是否进行有效性检查
 * redis.test-on-borrow=true
 * ## 连接池中的最大连接数
 * redis.jedis.pool.max-active=500
 * ## 连接池中的最大空闲连接
 * redis.jedis.pool.max-idle=500
 * ## 连接池最大阻塞等待时间（使用负值表示没有限制）
 * redis.jedis.pool.max-wait=500
 */
@Configuration
public class RedisConfig {

    @Value("${redis.test-on-borrow}")
    private boolean testOnBorrow;

    @Value("${redis.jedis.pool.max-idle}")
    private int maxIdle;

    @Value("${redis.jedis.pool.max-active}")
    private int maxActive;

    @Value("${redis.jedis.pool.max-wait}")
    private int maxWait;

    @Value("${redis.hosts}")
    private String hosts;

    @Value("${redis.mode}")
    private String mode;


    @Bean
    public JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxTotal(maxActive);
        jedisPoolConfig.setMaxWaitMillis(maxWait);
        jedisPoolConfig.setTestOnBorrow(testOnBorrow);
        return jedisPoolConfig;
    }

    @Bean
    public RedisConfiguration redisConfiguration() {
        String[] hostArray = hosts.split(",");
        Set<String> setHosts = new HashSet<>(hostArray.length);
        for (String host : hostArray) {
            setHosts.add(host);
        }
        RedisConfiguration redisConfiguration = null;
        switch (mode) {
            case "sentinel":
                redisConfiguration = new RedisSentinelConfiguration("mymaster", setHosts);
                break;
            case "cluster":
                redisConfiguration = new RedisClusterConfiguration(setHosts);
                break;
            case "standalone":
                redisConfiguration = new RedisStandaloneConfiguration(hosts,6379);
                break;

        }
        return redisConfiguration;
    }


    @Bean
    public JedisConnectionFactory jedisConnectionFactory(@Autowired JedisPoolConfig jedisPoolConfig, @Autowired RedisConfiguration redisConfiguration) {

        JedisConnectionFactory jedisConnectionFactory = null;
        if (redisConfiguration instanceof RedisSentinelConfiguration) {
            jedisConnectionFactory = new JedisConnectionFactory((RedisSentinelConfiguration) redisConfiguration, jedisPoolConfig);
        }
        if (redisConfiguration instanceof RedisClusterConfiguration) {
            jedisConnectionFactory = new JedisConnectionFactory((RedisClusterConfiguration) redisConfiguration, jedisPoolConfig);
        }
        if (redisConfiguration instanceof RedisStandaloneConfiguration) {
            jedisConnectionFactory = new JedisConnectionFactory((RedisStandaloneConfiguration)redisConfiguration);
        }
        jedisConnectionFactory.setUsePool(true);
        return jedisConnectionFactory;
    }

    @Bean
    public StringRedisTemplate redisTemplate(@Autowired JedisConnectionFactory jedisConnectionFactory) {
        StringRedisTemplate redisTemplate = new StringRedisTemplate();
        redisTemplate.setConnectionFactory(jedisConnectionFactory);
        return redisTemplate;
    }

    @Bean
    public RedisUtil redisUtil(@Autowired StringRedisTemplate redisTemplate) {
        RedisUtil redisUtil = new RedisUtil();
        redisUtil.setRedisTemplate(redisTemplate);
        return redisUtil;
    }

}
