package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    // ==================== 店铺缓存版本说明 ====================
    // V1：直接查询数据库，没有使用Redis缓存
    // V2：使用缓存空值解决缓存穿透问题 queryWithPassThrough()
    // V3：使用互斥锁解决缓存击穿问题 queryWithMutex()
    // V4：使用逻辑过期解决缓存击穿问题 queryWithLogicalExpire()
    // V5：把缓存穿透逻辑封装到CacheClient工具类
    // V6：把逻辑过期逻辑封装到CacheClient工具类【当前使用】
    // 整理时间：2026-08-18，只整理顺序和版本标记，保留原来的代码和注释
    // =========================================================

    @Override
    public Result queryById(Long id) {
        // ==================== V1 直接查询数据库（已停用） ====================
        // 停用原因：每次请求都查询数据库，并发高时数据库压力大
//        Shop shop = getById(id);

        // ==================== V2 缓存穿透解决（已停用） ====================
        // 优化：数据库不存在的数据也缓存空值，避免请求一直打到数据库
        // 停用原因：还没有解决热点key失效时的缓存击穿问题
//        Shop shop = queryWithPassThrough(id);//缓存穿透解决

        // ==================== V3 互斥锁解决缓存击穿（已停用） ====================
        // 优化：只允许一个线程查询数据库并重建缓存
        // 停用原因：其他线程需要等待，性能会受到影响
//        Shop shop = queryWithMutex(id);//缓存击穿解决

        // ==================== V4 逻辑过期解决缓存击穿（已停用） ====================
        // 优化：过期时先返回旧数据，再开启新线程重建缓存
        // 停用原因：这部分通用代码比较多，后面封装到了CacheClient中
//        Shop shop = queryWithLogicalExpire(id);//逻辑过期解决缓存击穿问题

        // ==================== V5 CacheClient缓存穿透版（已停用） ====================
//        工具类中封装的缓存逻辑来解决缓存穿透问题
//        Shop shop = cacheClient.queryWithPassThrough(
//                RedisConstants.CACHE_SHOP_KEY
//                , id
//                , Shop.class
//                , this::getById
//                , RedisConstants.CACHE_SHOP_TTL
//                , TimeUnit.MINUTES);

        // ==================== V6 CacheClient逻辑过期版（当前使用） ====================
        // 优化：业务层只负责传参数，逻辑过期、加锁和缓存重建由CacheClient完成
        // 注意：逻辑过期方案需要先使用saveShop2Redis()把店铺数据预热到Redis
        //工具类中封装的缓存逻辑来解决缓存击穿问题
        Shop shop = cacheClient.queryWithLogicalExpire(
                RedisConstants.CACHE_SHOP_KEY
                , id
                , Shop.class
                , this::getById
                , RedisConstants.CACHE_SHOP_TTL
                , TimeUnit.SECONDS
        );

        if (shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    // ==================== V2 缓存穿透版（已停用） ====================
    // 升级了什么：给不存在的店铺缓存空值，解决缓存穿透
    // 为什么停用：没有解决缓存击穿，后面升级为V3互斥锁方案
//queryWithPassThrough解决缓存穿透问题
    public Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
//        1.从Redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
//            3.存在，返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//请按照 Shop 类的字段，把这段 JSON 字符串转换成 Shop 对象
            return shop;
        }
        //判断是否是命中空值缓存
        if (shopJson!=null){
            return null;
        }

//        4.不存在，根据id查询数据库
        Shop shop = getById(id);
//        5.不存在，返回错误
        //还要把空值写入缓存，防止缓存穿透
        if (shop==null) {
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);//写入空值缓存，防止缓存穿透 2分钟有效期
            return null;
        }
//        6.存在，写入Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//shop对象转换成JSON字符串，再写入Redis
//        7.返回
        return shop;
    }

    // ==================== V3 互斥锁版（已停用） ====================
    // 升级了什么：同一时间只让一个线程重建缓存，解决缓存击穿
    // 为什么停用：没有拿到锁的线程要休眠重试，响应时间比较长，后面升级为V4逻辑过期
//    解决缓存击穿问题
    public Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
//        1.从Redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
//            3.存在，返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//请按照 Shop 类的字段，把这段 JSON 字符串转换成 Shop 对象
            return shop;
        }
        //判断是否是命中空值缓存
        if (shopJson!=null){
            return null;
        }
//        4实现缓存重建
//            4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY  + id;
        Shop shop=null;
        boolean isLock = false;
        try {
            isLock = tryLock(lockKey);
//            4.2判断是否获取成功
            if (!isLock) {
    //            4.3失败则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

//            4.4成功，根据id查询数据库
            shop = getById(id);
//        5.不存在，返回错误
            if (shop==null) {
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);//写入空值缓存，防止缓存穿透 2分钟有效期
                return null;
            }
//        6.存在，写入Redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//shop对象转换成JSON字符串，再写入Redis
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
//            7.释放互斥锁
            //【必要修正】只有当前线程拿到锁，才能释放锁，避免误删其他线程的锁
            if (isLock) {
                unLock(lockKey);
            }
        }

//        8.返回
        return shop;


    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);

        return BooleanUtil.isTrue(flag);
    }


    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    // ==================== V4 手写逻辑过期版（已停用） ====================
    // 升级了什么：缓存过期时先返回旧数据，再由独立线程重建缓存，减少用户等待
    // 为什么停用：代码比较通用且较多，后面统一封装到了CacheClient工具类
//    逻辑过期解决缓存击穿问题
    public void saveShop2Redis(Long id,Long expireSeconds) throws Exception {
//        1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
//        2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

//        3.保存到Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }


        //线程池
    private static final ExecutorService CACHR_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);//设置线程池



    public Shop queryWithLogicalExpire(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
//        1.从Redis查询商品缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//        2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
//            3.未命中，返回
            return null;
        }
//        4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
//        5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //        5.1未过期返回店铺信息
            return shop;
        }

//        5.2已过期，需要缓存重建


//        6.缓存重建

//        6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);


//            6.2 判断是否获取成功
        if (isLock) {
            String latestShopJson = stringRedisTemplate.opsForValue().get(key);

            if (StrUtil.isNotBlank(latestShopJson)) {
                RedisData latestRedisData =
                        JSONUtil.toBean(latestShopJson, RedisData.class);

                Shop latestShop = JSONUtil.toBean(
                        (JSONObject) latestRedisData.getData(),
                        Shop.class
                );

                // 其他线程已经完成重建，直接释放锁并返回
                if (latestRedisData.getExpireTime()
                        .isAfter(LocalDateTime.now())) {

                    unLock(lockKey);
                    return latestShop;
                }

                // 使用最新的旧数据返回
                shop = latestShop;
            }
//            6.3 成功，开启独立线程，实现缓存重建
            CACHR_REBUILD_EXECUTOR.submit(() -> {
                try {

                    //缓存重建
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {

                    //释放锁
                    unLock(lockKey);
                }

            });
        }else {
            //        6.5 失败，返回旧数据
            return shop;
        }


//                6.4 失败直接返回过期信息
        return shop;

    }





    // ==================== 店铺更新：数据库更新后删除缓存 ====================
    @Override
    @Transactional//添加事务
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id==null) {
            return Result.fail("店铺id不能为空");
        }
        //先更新数据库
        updateById(shop);
        //后删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}
