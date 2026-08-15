package com.hmdp.service.impl;

import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.interfaces.Func;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public List<ShopType> queryType() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
//        1.从Redis查询店铺类型缓存
        List<String> shopTypeJson = stringRedisTemplate.opsForList().range(key, 0, -1);
//        2.判断缓存是否存在
        if (shopTypeJson!=null && !shopTypeJson.isEmpty()) {
//            4.存在 将每个Json字符串转换成shopType对象 json转list
            return shopTypeJson.stream()
                    .map(Json -> JSONUtil.toBean(Json, ShopType.class))
                    .collect(Collectors.toList());
        }
//        3.不存在，查询数据库 3.1按照sort排序
        List<ShopType> typeList = query().orderByAsc("sort").list();
//        3.3数据库没有数据返回空列表
        if (typeList==null||typeList.isEmpty()) {
            return Collections.emptyList();
        }
//        3.2转成josn
        List<String> jsonList = typeList.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
//        3.3写入redis
        stringRedisTemplate.opsForList().rightPushAll(key, jsonList);

//        设置缓存有效期
        stringRedisTemplate.expire(key, RedisConstants.CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);

//        3.4返回店铺类型
        return typeList;

    }
}
