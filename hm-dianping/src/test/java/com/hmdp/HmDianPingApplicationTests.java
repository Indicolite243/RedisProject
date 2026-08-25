package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
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

@SpringBootTest(properties = {
        // 通用测试不验证异步消息链路，关闭后台消费者，避免抢走其他集成测试的消息。
        "hmdp.seckill.stream-direct-consumer-enabled=false",
        "hmdp.seckill.reservation-enabled=false",
        "hmdp.mq.stream-relay-enabled=false",
        "hmdp.mq.listener-enabled=false"
})
class HmDianPingApplicationTests {
    /**
     * 本轮压测使用的一次性登录身份数量。
     *
     * <p>每个身份只在 Redis 登录态和 JMeter CSV 中存在，用于模拟不同用户请求。
     * 不需要为了压测向 tb_user 写入大量无业务意义的测试数据。</p>
     */
    private static final int JMETER_TOKEN_COUNT = 10_000;
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
     * 为 JMeter 生成 10000 个互不重复的压测 Token。
     *
     * <p>先使用数据库中已有用户，再补充虚拟用户 ID。秒杀接口只从 Redis 登录态读取
     * UserDTO，不需要查询 tb_user，因此虚拟身份能模拟独立用户，又不会污染业务数据。</p>
     *
     * <p>Redis Key 格式：login:token:{token}；CSV 每行格式：token,userId。</p>
     */
    @Test
    void generateJmeterUserTokens() throws Exception {
        List<User> users = userService.list();
        List<String> csvLines = new ArrayList<>(JMETER_TOKEN_COUNT);
        long nextVirtualUserId = users.stream()
                .map(User::getId)
                .filter(java.util.Objects::nonNull)
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L) + 1;

        // 真实用户优先生成登录态，便于在需要时排查对应的数据库订单记录。
        for (User user : users) {
            if (csvLines.size() == JMETER_TOKEN_COUNT) {
                break;
            }
            writeJmeterToken(BeanUtil.copyProperties(user, UserDTO.class), csvLines);
        }

        // 数据库用户不足时，补齐虚拟压测身份。ID 从现有最大 ID 之后开始，避免与真实用户冲突。
        while (csvLines.size() < JMETER_TOKEN_COUNT) {
            UserDTO virtualUser = new UserDTO();
            virtualUser.setId(nextVirtualUserId++);
            virtualUser.setNickName("load-test-user-" + virtualUser.getId());
            virtualUser.setIcon("");
            writeJmeterToken(virtualUser, csvLines);
        }

        Path csvPath = Paths.get("target", "jmeter", "user-tokens.csv").toAbsolutePath();
        Files.createDirectories(csvPath.getParent());
        Files.write(csvPath, csvLines, StandardCharsets.UTF_8);

        System.out.println("Generated " + csvLines.size() + " unique JMeter tokens: " + csvPath);
    }

    /**
     * 将一个压测身份写入 Redis 登录态，并追加到 JMeter CSV。
     * Token 使用 userId 派生，重复执行时覆盖同一 Redis Key，便于反复压测。
     */
    private void writeJmeterToken(UserDTO userDTO, List<String> csvLines) {
        String token = "load-test-" + userDTO.getId();
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        Map<String, Object> userMap = BeanUtil.beanToMap(
                userDTO,
                new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())
        );

        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        csvLines.add(token + "," + userDTO.getId());
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
