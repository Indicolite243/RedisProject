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

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";

    //V4推模式Feed收件箱使用ZSet保存，key为feed:用户id，member为博客id，score为发布时间戳
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    public static final String SHOP_TYPE_GEO_KEY = "shop:geo:byType:";
}
