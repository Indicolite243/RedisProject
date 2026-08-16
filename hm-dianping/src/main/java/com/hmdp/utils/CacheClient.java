package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //普通缓存
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }
    //逻辑过期
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }
    //缓存穿透
//queryWithPassThrough解决缓存穿透问题
    public <R,ID> R queryWithPassThrough(String keyPrefix,ID id,Class<R> type, Function<ID,R> dbCallback,Long time, TimeUnit unit) {//有参数有返回值的类型叫做函数dbFallback数据查询的逻辑方法
        String key = keyPrefix + id;
//        1.从Redis查询商品缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
//        2.判断是否存在
        if (StrUtil.isNotBlank(Json)) {
//            3.存在，返回
            return JSONUtil.toBean(Json, type);
        }
        //判断是否是命中空值缓存
        if (Json!=null){
            return null;
        }

//        4.不存在，根据id查询数据库
        R r = dbCallback.apply(id);
//        5.不存在，返回错误
        //还要把空值写入缓存，防止缓存穿透
        if (r==null) {
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);//写入空值缓存，防止缓存穿透 2分钟有效期
            return null;
        }
//        6.存在，写入Redis
        this.set(key,r,time,unit);//this掉用普通缓存方法写入缓存
//        7.返回
        return r;
    }



    //线程池
    private static final ExecutorService CACHR_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);//设置线程池
    //尝试获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }


    public <R,ID> R queryWithLogicalExpire(String prefix,ID id,Class< R> type,Function<ID,R> dbCallback,Long time, TimeUnit unit) {
        String key = prefix + id;
//        1.从Redis查询商品缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
//        2.判断是否存在
        if (StrUtil.isBlank(Json)) {
//            3.未命中，返回
            return null;
        }
//        4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        R r= JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();
//        5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //        5.1未过期返回店铺信息
            return r;
        }

//        5.2已过期，需要缓存重建
//        6.缓存重建
//        6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);


//            6.2 判断是否获取成功
        if (isLock) {
            String latestJson = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isNotBlank(latestJson)) {
                RedisData latestRedisData =
                        JSONUtil.toBean(latestJson, RedisData.class);

                R latestr = JSONUtil.toBean(
                        (JSONObject) latestRedisData.getData(),
                        type
                );

                // 其他线程已经完成重建，直接释放锁并返回
                if (latestRedisData.getExpireTime()
                        .isAfter(LocalDateTime.now())) {

                    unLock(lockKey);
                    return latestr;
                }

                // 使用最新的旧数据返回
                r = latestr;
            }
//            6.3 成功，开启独立线程，实现缓存重建
            CACHR_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //缓存重建
                    R r1 = dbCallback.apply(id);
                    //写入Redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {

                    //释放锁
                    unLock(lockKey);
                }

            });
        }else {
            //        6.5 失败，返回旧数据
            return r;
        }
//                6.4 失败直接返回过期信息
        return r;

    }


}
