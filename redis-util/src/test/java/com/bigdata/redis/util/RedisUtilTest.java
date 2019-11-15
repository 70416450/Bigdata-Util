package com.bigdata.redis.util;

import com.bigdata.redis.conf.RedisConfig;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SpringBootTest()
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {RedisConfig.class})
@TestPropertySource(locations = {"classpath:redis-config.properties"})
class RedisUtilTest {

    @Autowired
    RedisUtil redisUtil;

    @Test
    void get() {
        System.out.println("atest:123--->>" + redisUtil.get("atest:123"));
    }

    @Test
    void set() {
        redisUtil.set("atest:123", "123123");
        redisUtil.set("atest:234", "123123");
    }

    @Test
    void delete() {
        redisUtil.delete("atest:123");
        System.out.println("atest:123--->>" + redisUtil.get("atest:123"));
    }

    @Test
    void deleteKeys() {
        Set<String> keysets = new HashSet<>(2);
        keysets.add("atest:123");
        keysets.add("atest:234");
        redisUtil.delete(keysets);
        System.out.println("atest:123--->>" + redisUtil.get("atest:123"));
        System.out.println("atest:234--->>" + redisUtil.get("atest:234"));
    }

    @Test
    void dump() {
        byte[] dump = redisUtil.dump("atest:123");
        System.out.println("atest:123序列化--->>" + new String(dump));
    }

    @Test
    void hasKey() {
        Boolean aBoolean = redisUtil.hasKey("atest:123");
        Boolean bBoolean = redisUtil.hasKey("atest:456");
        System.out.println("atest:123是否存在--->>" + aBoolean);
        System.out.println("atest:456是否存在--->>" + bBoolean);
    }

    @Test
    void expire() throws InterruptedException {
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
        redisUtil.expire("atest:123", 1, TimeUnit.MILLISECONDS);
        TimeUnit.SECONDS.sleep(2);
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
    }

    @Test
    void keys() {
        Set<String> keySet = redisUtil.keys("atest*");
        keySet.stream().forEach(System.out::println);
    }

    @Test
    void persist() throws InterruptedException {
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
        redisUtil.expire("atest:123", 3, TimeUnit.MILLISECONDS);
        redisUtil.persist("atest:123");
        TimeUnit.SECONDS.sleep(4);
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
    }

    @Test
    void getExpire() throws InterruptedException {
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
        redisUtil.expire("atest:123", 3, TimeUnit.MILLISECONDS);
        Long expire = redisUtil.getExpire("atest:123", TimeUnit.MILLISECONDS);
        System.out.println("atest:123剩余过期时间--->>" + expire);
        redisUtil.persist("atest:123");
        TimeUnit.SECONDS.sleep(4);
        System.out.println("atest:123是否存在--->>" + redisUtil.hasKey("atest:123"));
    }

    @Test
    void lLeftPush() throws InterruptedException {
        redisUtil.lLeftPush("atest:123", "123123");
        redisUtil.lLeftPushIfPresent("atest:234", "234234");
        redisUtil.lLeftPush("atest:345", "atest:123", "345345");
    }

    @Test
    void putListCacheWithExpireTime() {
        ArrayList<Object> objects = new ArrayList<>(2);
        objects.add(new Car(1, "car1"));
        objects.add(new Car(2, "car2"));
        boolean atest = redisUtil.putListCacheWithExpireTime("atest", objects, 10000000);
        System.out.println(atest);
        List<Car> atest1 = redisUtil.getListCache("atest", Car.class);
        System.out.println(atest1);
    }

}