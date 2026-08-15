package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

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
    @Override
    public Result queryById(Long id) {
//        Shop shop = queryWithPassThrough(id);//缓存穿透解决
        Shop shop = queryWithMutex(id);//缓存击穿解决
        if (shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

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
        String lockKey = "lock:shop:" + id;
        Shop shop=null;
        try {
            boolean isLock = tryLock(lockKey);
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
            unLock(lockKey);
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
