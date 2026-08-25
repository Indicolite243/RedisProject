package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 30000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String CACHE_SHOP_TYPE_KEY = "cache:shoptype:list";
    public static final Long CACHE_SHOP_TYPE_TTL = 30L;

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    //V2关注列表使用Set保存，key为follows:用户id，member为被关注用户id
    public static final String FOLLOWS_KEY = "follows:";

    /**
     * 秒杀库存：String，完整 Key 为 {@code seckill:stock:{voucherId}}，value 为剩余库存。
     * 库存由秒杀 Lua 原子预扣，订单事务落库时还会独立扣减 MySQL 库存。
     */
    public static final String SECKILL_STOCK_KEY = "seckill:stock:";

    /**
     * 一人一单映射：Hash，完整 Key 为 {@code seckill:order-map:{voucherId}}。
     * field 是 userId，value 是 orderId；补偿成功后才会删除对应 field。
     */
    public static final String SECKILL_ORDER_MAP_KEY = "seckill:order-map:";

    /**
     * 单笔订单的预占记录：Hash，完整 Key 为 {@code seckill:reservation:{orderId}}。
     * 保存订单标识、用户、优惠券、发布次数、下次重试时间和业务状态。
     */
    public static final String SECKILL_RESERVATION_KEY = "seckill:reservation:";

    /**
     * RabbitMQ 待发布索引：ZSet，member 为 orderId，score 为 nextRetryAt 毫秒时间戳。
     * Dispatcher 只扫描 score 小于等于当前时间的成员。
     */
    public static final String SECKILL_PUBLISH_PENDING_KEY = "seckill:publish:pending";


    public static final String BLOG_LIKED_KEY = "blog:liked:";

    //V4推模式Feed收件箱使用ZSet保存，key为feed:用户id，member为博客id，score为发布时间戳
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    public static final String SHOP_TYPE_GEO_KEY = "shop:geo:byType:";
}
