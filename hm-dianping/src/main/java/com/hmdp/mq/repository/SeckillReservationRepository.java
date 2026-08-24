package com.hmdp.mq.repository;


import cn.hutool.core.bean.BeanUtil;
import com.hmdp.entity.SeckillReservation;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import org.springframework.core.io.ClassPathResource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;



@Repository
public class SeckillReservationRepository {

    private static final DefaultRedisScript<Long>
            UPDATE_PUBLISH_STATUS_SCRIPT;

    static {
        UPDATE_PUBLISH_STATUS_SCRIPT = new DefaultRedisScript<>();
        UPDATE_PUBLISH_STATUS_SCRIPT.setLocation(
                new ClassPathResource("update_publish_status.lua")
        );
        UPDATE_PUBLISH_STATUS_SCRIPT.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;

    public SeckillReservationRepository(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //    根据订单id读取Redis中的预占记录
    public SeckillReservation findByOrderId(Long orderId) {
        String key = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        Map<Object, Object> values = stringRedisTemplate.opsForHash().entries(key);

        if (values.isEmpty()){
            return null;
        }
//          Hutool会把Hash字段转换到实体类的同名属性中
//        作用是把Map中的数据填充到实体类，然后返回填充完成的对象。
        return BeanUtil.fillBeanWithMap(values,new SeckillReservation(),false);
    }

    //    根据订单id删除Redis中的预占记录
    public Set<String> findPendingOrders(long now,int  limit) {
        Set<String> orderIds = stringRedisTemplate.opsForZSet()
                .rangeByScore(RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                        0, now, 0, limit);

        return orderIds ==null? Collections.emptySet():orderIds;
    }

//    RabbitMQ确认收到消息后，将状态改为PUBLISHED，并从待发送ZSet中移除。
public void markPublished(Long orderId) {
    String reservationKey =
            RedisConstants.SECKILL_RESERVATION_KEY + orderId;

    stringRedisTemplate.execute(
            UPDATE_PUBLISH_STATUS_SCRIPT,
            Arrays.asList(
                    reservationKey,
                    RedisConstants.SECKILL_PUBLISH_PENDING_KEY
            ),
            orderId.toString(), // ARGV[1]
            "PUBLISHED",        // ARGV[2]
            "0"                 // ARGV[3]，成功时不使用
    );
}
    //        RabbitMQ发送失败后，记录重试次数和下次发送时间。
    public long scheduleRetry(Long orderId, long nextRetryAt, int maxAttempts) {
        String reservationKey =
                RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        Long result = stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY
                ),
                orderId.toString(),             // ARGV[1]
                "RETRY",                        // ARGV[2]
                String.valueOf(nextRetryAt),// ARGV[3]
                String.valueOf(maxAttempts)// ARGV[4]
        );
        return result == null ? 0L : result;
    }
    /**
     * 从待发送ZSet中移除订单。
     */
    public void removePending(Long orderId) {
        stringRedisTemplate.opsForZSet().remove(
                RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                orderId.toString()
        );
    }

    public void markCreated(Long orderId){
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(reservationKey,RedisConstants.SECKILL_PUBLISH_PENDING_KEY),
                orderId.toString(),
                "CREATED",
                "0"
        );
    }

    /**
     * 将DEAD订单重新加入待发布队列。
     *
     * @return true：重新入队成功；false：订单不存在或当前不是DEAD状态
     */


//        Java调用 requeueDead(orderId)
//        ↓
//        Lua检查 status是否为DEAD
//        ↓
//        改为PUBLISH_RETRY并清零重试次数
//        ↓
//        重新加入pending ZSet
//        ↓
//        返回1，Java得到true
    public boolean requeueDead(Long orderId) {
        String reservationKey =
                RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        // 使用当前时间作为ZSet分数，Dispatcher可以立即扫描到它
        long now = System.currentTimeMillis();

        Long result = stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY
                ),
                orderId.toString(),       // ARGV[1]
                "REQUEUE",                // ARGV[2]
                String.valueOf(now),      // ARGV[3]
                "0"                       // ARGV[4]，REQUEUE动作不使用
        );
        return Long.valueOf(1L).equals(result);//result==1返回true result==null 返回false
    }

    /**
     * 原子补偿一条DEAD预占记录。
     *
     * 返回值：
     *  1：补偿成功或已经补偿
     *  0：当前状态不是DEAD
     * -2：用户订单映射不匹配
     * -3：Redis库存数据异常
     */
    public long compensate(SeckillReservation reservation) {
        Long orderId = reservation.getOrderId();
        Long voucherId = reservation.getVoucherId();

        String reservationKey =RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        String stockKey =RedisConstants.SECKILL_STOCK_KEY + voucherId;

        String orderMapKey = RedisConstants.SECKILL_ORDER_MAP_KEY + voucherId;

        Long result = stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,                              // KEYS[1]
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY, // KEYS[2]
                        stockKey,                                   // KEYS[3]
                        orderMapKey                                 // KEYS[4]
                ),
                orderId.toString(), // ARGV[1]
                "COMPENSATE",       // ARGV[2]
                "0",                // ARGV[3]，补偿时不使用
                "0"                 // ARGV[4]，补偿时不使用
        );

        return result == null ? 0L : result;
    }
}
