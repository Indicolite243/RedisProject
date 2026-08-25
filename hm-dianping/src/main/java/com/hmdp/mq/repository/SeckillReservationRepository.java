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
/**
 * 秒杀预占记录的 Redis 数据访问层。
 *
 * <p>这个类只负责读写 Redis，不负责决定业务流程。它管理两类核心数据：</p>
 * <ul>
 *     <li>{@code seckill:reservation:{orderId}}：Hash，保存一笔订单的预占信息和当前状态。</li>
 *     <li>{@code seckill:publish:pending}：ZSet，member 是 orderId，score 是下次允许发送的时间。</li>
 * </ul>
 *
 * <p>涉及“修改 Reservation 状态并同步修改 Pending ZSet”的操作统一交给
 * {@code update_publish_status.lua}。这样多个 Redis 命令会作为一个整体执行，
 * 避免应用在两个命令之间宕机，留下状态和待发送集合不一致的数据。</p>
 */
@Repository
public class SeckillReservationRepository {

    /**
     * 预占状态迁移脚本。
     *
     * <p>同一份脚本通过 action 参数处理 PUBLISHED、RETRY、CREATED、REQUEUE
     * 和 COMPENSATE，状态校验与数据修改都在 Redis 内原子完成。</p>
     */
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

    /**
     * 根据订单 ID 读取 Redis 中的预占记录。
     *
     * @return 预占记录；Hash 不存在或没有字段时返回 {@code null}
     */
    public SeckillReservation findByOrderId(Long orderId) {
        String key = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        Map<Object, Object> values = stringRedisTemplate.opsForHash().entries(key);

        if (values.isEmpty()) {
            return null;
        }

        // Hutool 按字段名把 Hash 中的字符串值转换并填充到实体类同名属性中。
        // 第三个参数 false 表示字段名区分大小写，例如 orderId 只匹配实体的 orderId 属性。
        return BeanUtil.fillBeanWithMap(values, new SeckillReservation(), false);
    }

    /**
     * 查询已经到达发送时间的订单 ID。
     *
     * <p>ZSet 的 score 是 {@code nextRetryAt}。查询范围 {@code 0..now}
     * 表示只取当前时刻已经可以发布或重试的订单，limit 用来限制单次扫描数量。</p>
     */
    public Set<String> findPendingOrders(long now, int limit) {
        Set<String> orderIds = stringRedisTemplate.opsForZSet()
                .rangeByScore(RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                        0, now, 0, limit);

        // Spring Data Redis 在没有结果时可能返回 null，统一转换为空集合可简化 Dispatcher 循环。
        return orderIds == null ? Collections.emptySet() : orderIds;
    }

    /**
     * RabbitMQ Confirm 成功后，把预占状态改为 PUBLISHED，并移出待发送 ZSet。
     *
     * <p>必须在发布者确认 Broker 已接收消息后调用。脚本会阻止 CREATED 等终态
     * 被较晚到达的发布回调覆盖。</p>
     */
    public void markPublished(Long orderId) {
        String reservationKey =
                RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY
                ),
                orderId.toString(), // ARGV[1]：ZSet member，同时用于日志对应订单
                "PUBLISHED",        // ARGV[2]：要求 Lua 执行“发布成功”状态迁移
                "0"                 // ARGV[3]：PUBLISHED 动作不需要重试时间，占位即可
        );
    }

    /**
     * RabbitMQ 发布失败后记录一次尝试，并安排下次重试时间。
     *
     * @return 1 表示已进入 PUBLISH_RETRY；2 表示达到上限并进入 DEAD；
     *         0 表示记录不存在或当前状态已经不允许重试
     */
    public long scheduleRetry(Long orderId, long nextRetryAt, int maxAttempts) {
        String reservationKey =
                RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        Long result = stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY
                ),
                orderId.toString(),              // ARGV[1]：订单 ID
                "RETRY",                        // ARGV[2]：执行失败重试分支
                String.valueOf(nextRetryAt),     // ARGV[3]：下一次可扫描到该订单的时间
                String.valueOf(maxAttempts)      // ARGV[4]：达到该次数后转为 DEAD
        );
        return result == null ? 0L : result;
    }

    /**
     * 从待发送 ZSet 中移除订单。
     *
     * <p>这是清理异常数据的辅助方法。正常状态迁移优先调用 Lua 方法，
     * 让 Reservation 与 ZSet 在同一次原子操作中更新。</p>
     */
    public void removePending(Long orderId) {
        stringRedisTemplate.opsForZSet().remove(
                RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                orderId.toString()
        );
    }

    /**
     * MySQL 事务成功后把预占记录标记为 CREATED。
     *
     * <p>此状态说明最终订单已经落库，同时移除任何可能残留的待发布记录。</p>
     */
    public void markCreated(Long orderId) {
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(reservationKey, RedisConstants.SECKILL_PUBLISH_PENDING_KEY),
                orderId.toString(), // ARGV[1]：订单 ID
                "CREATED",         // ARGV[2]：执行订单落库完成分支
                "0"                // ARGV[3]：CREATED 动作不需要重试时间
        );
    }

    /**
     * 将 DEAD 订单人工重新加入待发布队列。
     *
     * @return {@code true} 表示重新入队成功；{@code false} 表示订单不存在或当前不是 DEAD 状态
     */
    public boolean requeueDead(Long orderId) {
        String reservationKey =
                RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        // 使用当前时间作为 ZSet 分数，Dispatcher 下一轮扫描时就能立即取到它。
        long now = System.currentTimeMillis();

        Long result = stringRedisTemplate.execute(
                UPDATE_PUBLISH_STATUS_SCRIPT,
                Arrays.asList(
                        reservationKey,
                        RedisConstants.SECKILL_PUBLISH_PENDING_KEY
                ),
                orderId.toString(),       // ARGV[1]：订单 ID
                "REQUEUE",               // ARGV[2]：执行人工重新投递分支
                String.valueOf(now),      // ARGV[3]：立即允许 Dispatcher 扫描
                "0"                      // ARGV[4]：REQUEUE 动作不使用
        );
        // 使用 equals 同时处理 result 为 null 的情况，只有 Lua 明确返回 1 才算成功。
        return Long.valueOf(1L).equals(result);
    }

    /**
     * 原子补偿一条 DEAD 预占记录。
     *
     * <p>Lua 会在同一次执行中恢复 Redis 库存、删除用户的一人一单映射、
     * 移除 Pending 成员并将状态改成 COMPENSATED，因此重复调用不会重复加库存。</p>
     *
     * @return 1 表示补偿成功或此前已补偿；0 表示当前状态不是 DEAD；
     *         -2 表示用户订单映射不匹配；-3 表示 Redis 库存数据异常
     */
    public long compensate(SeckillReservation reservation) {
        Long orderId = reservation.getOrderId();
        Long voucherId = reservation.getVoucherId();

        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;

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
