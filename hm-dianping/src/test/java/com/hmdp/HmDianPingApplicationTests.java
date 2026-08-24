package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        // 通用测试不验证异步消息链路，关闭后台消费者，避免抢走其他集成测试的消息。
        "hmdp.seckill.stream-direct-consumer-enabled=false",
        "hmdp.seckill.reservation-enabled=false",
        "hmdp.mq.stream-relay-enabled=false",
        "hmdp.mq.listener-enabled=false"
})
class HmDianPingApplicationTests {
    @Resource
    private CacheClient cacheClient;
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHR_REBUILD_EXECUTOR = Executors.newFixedThreadPool(500);

    @Test
    void  testSave() throws Exception {
        shopService.saveShop2Redis(1L,10L);
    }

    @Test
    void testQueryWithPassThrough() {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);

    }

    @Test
    void testIDWorker() throws Exception {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " +id);
            }
            latch.countDown();
         };
        long begin = System.currentTimeMillis();

        for (int i = 0; i < 300; i++) {
            CACHR_REBUILD_EXECUTOR.submit(task);
        }

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));


    }

    /**
     * 为 JMeter 生成 1000 个可登录用户的 Token。
     *
     * <p>Redis Key 格式与正常登录保持一致：login:token:{token}。
     * CSV 每行格式为 token,userId，可在 JMeter 的 CSV Data Set Config 中读取。</p>
     */
    @Test
    void generateJmeterUserTokens() throws Exception {
        List<User> users = userService.list(
                new QueryWrapper<User>()
                        .orderByAsc("id")
                        .last("LIMIT 1000")
        );
        assertEquals(1000, users.size(), "数据库中的用户数量不足 1000 个");

        List<String> csvLines = new ArrayList<>(users.size());
        for (User user : users) {
            // 使用确定性 Token，重复执行测试不会在 Redis 中不断堆积新 Key。
            String token = "load-test-" + user.getId();
            String tokenKey = RedisConstants.LOGIN_USER_KEY + token;

            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(
                    userDTO,
                    new HashMap<>(),
                    CopyOptions.create()
                            .setIgnoreNullValue(true)
                            .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
            );

            stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
            stringRedisTemplate.expire(
                    tokenKey,
                    RedisConstants.LOGIN_USER_TTL,
                    TimeUnit.MINUTES
            );
            csvLines.add(token + "," + user.getId());
        }

        Path csvPath = Paths.get("target", "jmeter", "user-tokens.csv").toAbsolutePath();
        Files.createDirectories(csvPath.getParent());
        Files.write(csvPath, csvLines, StandardCharsets.UTF_8);

        System.out.println("Generated " + csvLines.size() + " tokens: " + csvPath);
    }

    @Test
    void loadShopDate(){
        //查询所有店铺信息
        List<Shop> list = shopService.list();
        //把店铺分组，按照type_id分组，分批完成写入
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));

        //分批写入Redis
        for(Map.Entry<Long, List<Shop>> entry:map.entrySet()){
            //获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            List<Shop> value = entry.getValue();

            //写入Redis GEOSADD key longitude latitude member
            for (Shop shop : value) {
                stringRedisTemplate.opsForGeo()
                        .add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
            }
        }
    }
    @Test
    void testHyperloglog(){
        String[] values=new String[1000];
        int j=0;
        for (int i = 0; i < 1000000; i++) {
            j=i%1000;
            values[j]="user_"+i;
            if (j==999){
                stringRedisTemplate.opsForHyperLogLog().add("hil",values);
            }
        }
        Long cont = stringRedisTemplate.opsForHyperLogLog().size("hil");
        System.out.println("cont = " + cont);

    }




}
