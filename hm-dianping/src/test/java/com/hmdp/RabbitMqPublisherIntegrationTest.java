package com.hmdp;

import com.hmdp.dto.SeckillOrderMessageDTO;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.dto.Result;
import com.hmdp.mq.ConfirmedSeckillOrderPublisher;
import com.hmdp.mq.SeckillMqConstants;
import com.hmdp.mq.SeckillReservationCompensationService;
import com.hmdp.mq.repository.SeckillReservationRepository;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RabbitMQ 秒杀可靠链路集成测试。
 *
 * <p>该测试会启动完整的 Spring Boot 测试容器，并连接真实 Redis 和 RabbitMQ，验证：</p>
 *
 * <ol>
 *     <li>Publisher Confirm、消息路由和 JSON 反序列化。</li>
 *     <li>DEAD 记录人工重新投递及状态约束。</li>
 *     <li>库存补偿的原子性与重复补偿幂等性。</li>
 *     <li>订单状态查询和订单归属校验。</li>
 * </ol>
 *
 * <p>Redis 使用 DB15，并在每个用例结束后清理测试 Key；RabbitMQ Listener 在测试中关闭，
 * 防止测试消息被后台消费者抢先取走。运行前必须确保 Redis 和 RabbitMQ 可连接。</p>
 */
@SpringBootTest(properties = {
        "hmdp.seckill.reservation-enabled=false",
        "hmdp.mq.listener-enabled=false",
        "spring.redis.database=15"
})
public class RabbitMqPublisherIntegrationTest {

    @Autowired
    private ConfirmedSeckillOrderPublisher publisher;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private SeckillReservationRepository reservationRepository;

    @Autowired
    private SeckillReservationCompensationService compensationService;

    @Autowired
    private IVoucherOrderService voucherOrderService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 验证一条 DTO 能收到 Confirm ACK、正确路由到主队列并按原类型反序列化。
     */
    @Test
    void shouldPublishAndReceiveSeckillOrderMessage() throws Exception {
        // 使用项目现有全局 ID 生成器，只生成 ID，不创建数据库订单。
        long orderId = redisIdWorker.nextId("order");
        long createdAt = System.currentTimeMillis();
//        构造一条秒杀订单消息。
        SeckillOrderMessageDTO payload = new SeckillOrderMessageDTO(
                "seckill-order-" + orderId,//业务层面的消息唯一 ID，用于消息幂等处理。
                orderId,//秒杀订单 ID。
                1L,//下单用户 ID。
                1L,//下单用户 ID。
                createdAt,//消息创建时间。
                1//消息结构版本号，用于以后兼容不同版本的消息格式。
        );

        // Confirm ACK 且未 Return 才会正常返回。
        publisher.publishAndAwait(payload);

        // basic.get 方式取走测试消息；不会启动常驻消费者。
        Object received = rabbitTemplate.receiveAndConvert(//从目标队列中主动获取消息。
                SeckillMqConstants.ORDER_QUEUE,//要读取的队列名称。
                3000//最长等待时间，单位是毫秒。
        );

        assertNotNull(received, "3秒内未从主队列获取到消息");//断言 3 秒内确实读取到了消息。
        assertTrue(
                received instanceof SeckillOrderMessageDTO,//断言 RabbitTemplate 已经将 JSON 消息反序列化位SeckillOrderMessageDTO 类型
                "JSON 未正确反序列化为 SeckillOrderMessageDTO"
        );

        SeckillOrderMessageDTO actual = (SeckillOrderMessageDTO) received;//类型断言通过后，将接收到的对象转换为 DTO。
        assertEquals(payload.getMessageId(), actual.getMessageId());//验证消息 ID 一致。
        assertEquals(payload.getOrderId(), actual.getOrderId());
        assertEquals(payload.getUserId(), actual.getUserId());
        assertEquals(payload.getVoucherId(), actual.getVoucherId());
        assertEquals(payload.getSchemaVersion(), actual.getSchemaVersion());
    }

    @Test
    void shouldRequeueDeadReservation() {
        Long orderId = 999999999999L;
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        // 测试使用Redis DB15，并在结束时清理，不影响开发环境的秒杀数据。
        stringRedisTemplate.delete(reservationKey);
        stringRedisTemplate.opsForZSet().remove(
                RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                orderId.toString()
        );

        try {
            // 准备一条已经达到重试上限的预占记录。
            stringRedisTemplate.opsForHash().put(reservationKey, "status", "DEAD");
            stringRedisTemplate.opsForHash().put(reservationKey, "publishAttempts", "5");

            boolean requeued = reservationRepository.requeueDead(orderId);

            assertTrue(requeued);
            assertEquals(
                    "PUBLISH_RETRY",
                    stringRedisTemplate.opsForHash().get(reservationKey, "status")
            );
            assertEquals(
                    "0",
                    stringRedisTemplate.opsForHash().get(reservationKey, "publishAttempts")
            );
            assertNotNull(
                    stringRedisTemplate.opsForZSet().score(
                            RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                            orderId.toString()
                    )
            );

            // 第一次执行后状态已经不是DEAD，第二次重新入队应当被拒绝。
            assertFalse(reservationRepository.requeueDead(orderId));
        } finally {
            stringRedisTemplate.delete(reservationKey);
            stringRedisTemplate.opsForZSet().remove(
                    RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                    orderId.toString()
            );
        }
    }

    @Test
    void shouldCompensateDeadReservationOnlyOnce() {
        Long orderId = 999999999998L;
        Long userId = 987654322L;
        Long voucherId = 987654321L;
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        String orderMapKey = RedisConstants.SECKILL_ORDER_MAP_KEY + voucherId;

        // 准备一条消息发布失败、已经进入 DEAD 状态的预占记录。
        stringRedisTemplate.opsForHash().put(reservationKey, "orderId", orderId.toString());
        stringRedisTemplate.opsForHash().put(reservationKey, "messageId", "seckill-order-" + orderId);
        stringRedisTemplate.opsForHash().put(reservationKey, "userId", userId.toString());
        stringRedisTemplate.opsForHash().put(reservationKey, "voucherId", voucherId.toString());
        stringRedisTemplate.opsForHash().put(reservationKey, "status", "DEAD");
        stringRedisTemplate.opsForHash().put(reservationKey, "publishAttempts", "5");
        stringRedisTemplate.opsForHash().put(reservationKey, "nextRetryAt", "0");
        stringRedisTemplate.opsForHash().put(
                reservationKey,
                "createdAt",
                Long.toString(System.currentTimeMillis())
        );
        stringRedisTemplate.opsForValue().set(stockKey, "10");
        stringRedisTemplate.opsForHash().put(orderMapKey, userId.toString(), orderId.toString());
        stringRedisTemplate.opsForZSet().add(
                RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                orderId.toString(),
                0
        );

        try {
            long result = compensationService.compensate(orderId);

            assertEquals(1L, result);
            assertEquals("11", stringRedisTemplate.opsForValue().get(stockKey));
            assertNull(stringRedisTemplate.opsForHash().get(orderMapKey, userId.toString()));
            assertEquals(
                    "COMPENSATED",
                    stringRedisTemplate.opsForHash().get(reservationKey, "status")
            );
            assertNull(
                    stringRedisTemplate.opsForZSet().score(
                            RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                            orderId.toString()
                    )
            );

            // 重复补偿应直接返回成功，库存不能再次增加。
            assertEquals(1L, compensationService.compensate(orderId));
            assertEquals("11", stringRedisTemplate.opsForValue().get(stockKey));
        } finally {
            stringRedisTemplate.delete(reservationKey);
            stringRedisTemplate.delete(stockKey);
            stringRedisTemplate.delete(orderMapKey);
            stringRedisTemplate.opsForZSet().remove(
                    RedisConstants.SECKILL_PUBLISH_PENDING_KEY,
                    orderId.toString()
            );
        }
    }

    @Test
    void shouldQueryReservationStatusAndCheckOwner() {
        Long orderId = 999999999997L;
        Long userId = 987654323L;
        String reservationKey = RedisConstants.SECKILL_RESERVATION_KEY + orderId;

        // MySQL中不存在这个测试订单，因此查询会继续读取Redis Reservation。
        stringRedisTemplate.opsForHash().put(reservationKey, "orderId", orderId.toString());
        stringRedisTemplate.opsForHash().put(reservationKey, "messageId", "seckill-order-" + orderId);
        stringRedisTemplate.opsForHash().put(reservationKey, "userId", userId.toString());
        stringRedisTemplate.opsForHash().put(reservationKey, "voucherId", "987654321");
        stringRedisTemplate.opsForHash().put(reservationKey, "status", "PUBLISH_RETRY");
        stringRedisTemplate.opsForHash().put(reservationKey, "publishAttempts", "1");
        stringRedisTemplate.opsForHash().put(reservationKey, "nextRetryAt", "0");
        stringRedisTemplate.opsForHash().put(
                reservationKey,
                "createdAt",
                Long.toString(System.currentTimeMillis())
        );

        try {
            Result processingResult = voucherOrderService
                    .querySeckillOrderStatus(orderId, userId);
            assertTrue(processingResult.getSuccess());
            SeckillOrderStatusDTO processingStatus =
                    (SeckillOrderStatusDTO) processingResult.getData();
            assertEquals("PROCESSING", processingStatus.getStatus());

            // DEAD表示自动投递已经停止，需要人工决定重放还是补偿。
            stringRedisTemplate.opsForHash().put(reservationKey, "status", "DEAD");
            Result failedResult = voucherOrderService
                    .querySeckillOrderStatus(orderId, userId);
            SeckillOrderStatusDTO failedStatus =
                    (SeckillOrderStatusDTO) failedResult.getData();
            assertEquals("FAILED", failedStatus.getStatus());

            // 补偿完成后才可以明确告诉用户能够重新抢购。
            stringRedisTemplate.opsForHash().put(reservationKey, "status", "COMPENSATED");
            Result compensatedResult = voucherOrderService
                    .querySeckillOrderStatus(orderId, userId);
            SeckillOrderStatusDTO compensatedStatus =
                    (SeckillOrderStatusDTO) compensatedResult.getData();
            assertEquals("COMPENSATED", compensatedStatus.getStatus());

            // 换成其他用户查询同一个orderId，接口必须拒绝返回订单状态。
            Result otherUserResult = voucherOrderService
                    .querySeckillOrderStatus(orderId, userId + 1);
            assertFalse(otherUserResult.getSuccess());
            assertEquals("无权查询该订单", otherUserResult.getErrorMsg());
        } finally {
            stringRedisTemplate.delete(reservationKey);
        }
    }
}
