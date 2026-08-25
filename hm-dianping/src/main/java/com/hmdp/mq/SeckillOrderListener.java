package com.hmdp.mq;


import com.hmdp.dto.SeckillOrderMessageDTO;
import com.hmdp.entity.SeckillReservation;
import com.hmdp.mq.repository.SeckillReservationRepository;
import com.hmdp.service.IVoucherOrderTransactionalService;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
/**
 * RabbitMQ 秒杀订单消费者。
 *
 * <p>它负责把已经通过 Redis 资格校验的订单最终写入 MySQL。RabbitMQ 采用“至少一次”
 * 投递语义，网络抖动、消费者异常或 ACK 丢失都可能让同一消息再次到达，因此这里必须
 * 同时依靠订单锁、业务幂等查询和数据库唯一约束防止重复订单。</p>
 *
 * <p>方法正常返回时，AUTO ACK 会确认消息；方法抛出异常时，由 Spring Retry 重试，
 * 重试耗尽后按照配置拒绝消息并转入 DLQ。</p>
 */
@Component
@ConditionalOnProperty(
        prefix = "hmdp.mq",
        name = "listener-enabled",
        havingValue = "true"
)
public class SeckillOrderListener {

    /** 为同一 orderId 的消费和补偿提供分布式互斥。 */
    private final RedissonClient redissonClient;

    /** 当前类专用日志记录器，便于按 Listener 类名筛选消费日志。 */
    private static final Logger LOGGER=LoggerFactory.getLogger(SeckillOrderListener.class);

    /** 执行 MySQL 订单保存和库存扣减事务。 */
    private final IVoucherOrderTransactionalService transactionalService;

    /** 读取并推进 Redis Reservation 状态。 */
    private final SeckillReservationRepository reservationRepository;

    public SeckillOrderListener(RedissonClient redissonClient, IVoucherOrderTransactionalService transactionalService, SeckillReservationRepository reservationRepository) {
        this.redissonClient = redissonClient;
        this.transactionalService = transactionalService;
        this.reservationRepository = reservationRepository;
    }

    /**
     * 消费一条秒杀订单消息。
     *
     * @param message Jackson 消息转换器反序列化得到的订单 DTO
     */
    @RabbitListener(queues = SeckillMqConstants.ORDER_QUEUE)
    public void onMessage(SeckillOrderMessageDTO message){
        LOGGER.info("收到RabbitMQ秒杀订单消息消息：{}",message);

        // Listener 和补偿服务必须使用完全相同的锁 Key。
        // 否则可能一边创建 MySQL 订单，一边恢复 Redis 库存，造成一笔订单对应两份库存。
        RLock lock = redissonClient.getLock(
                "lock:seckill:order:" + message.getOrderId()
        );

        // lock() 会等待到获取锁，并由 Redisson 看门狗在任务未结束时自动续期。
        lock.lock();
        try {
            // 获取锁后再读取最新状态，避免等待锁期间状态已经被补偿线程修改。
            SeckillReservation reservation =
                    reservationRepository.findByOrderId(message.getOrderId());

            // 第二阶段消息必须存在对应的 Redis 预占记录。缺失表示链路数据不完整，
            // 不能直接落库；抛异常后交给重试和 DLQ 保留问题现场。
            if (reservation == null) {
                throw new IllegalStateException("找不到秒杀订单预占记录，orderId="+ message.getOrderId());
            }
            String status = reservation.getStatus();

            // 库存已经开始补偿或补偿完成时，晚到的 MQ 消息必须被忽略。
            // 正常 return 会 ACK 这条过期消息，防止它反复投递。
            if (SeckillReservation.STATUS_COMPENSATING.equals(status)
                    || SeckillReservation.STATUS_COMPENSATED.equals(status)) {
                LOGGER.warn("订单已经进入补偿流程，忽略晚到消息，orderId={}, status={}",message.getOrderId(),status);
                return;
            }

            // 在同一把订单锁内先完成 MySQL 事务，再推进 Redis 状态。
            // 若 Redis 状态更新失败，消息会重试；createOrder 的幂等校验会避免重复写库。
            transactionalService.createOrder(message);
            reservationRepository.markCreated(message.getOrderId());

            LOGGER.info("处理完成RabbitMQ秒杀订单，orderId={}",message.getOrderId());
        } finally {
            // 无论成功还是异常都释放锁；异常继续向外传播，由 Rabbit Listener 容器处理重试。
            lock.unlock();
        }
    }

}
