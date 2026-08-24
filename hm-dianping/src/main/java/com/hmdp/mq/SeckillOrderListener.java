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
 * RabbitMQ秒杀订单消费者。
 */
@Component
@ConditionalOnProperty(
        prefix = "hmdp.mq",
        name = "listener-enabled",
        havingValue = "true"
)
public class SeckillOrderListener {

    private final RedissonClient redissonClient;
//    创建当前类专用的日志记录器：
    private static final Logger LOGGER=LoggerFactory.getLogger(SeckillOrderListener.class);

    private final IVoucherOrderTransactionalService transactionalService;

    private final SeckillReservationRepository reservationRepository;

    public SeckillOrderListener(RedissonClient redissonClient, IVoucherOrderTransactionalService transactionalService, SeckillReservationRepository reservationRepository) {
        this.redissonClient = redissonClient;
        this.transactionalService = transactionalService;
        this.reservationRepository = reservationRepository;
    }

    @RabbitListener(queues = SeckillMqConstants.ORDER_QUEUE)
    public void onMessage(SeckillOrderMessageDTO message){
        LOGGER.info("收到RabbitMQ秒杀订单消息消息：{}",message);

        // Listener和补偿服务必须使用相同的锁Key
        RLock lock = redissonClient.getLock(
                "lock:seckill:order:" + message.getOrderId()
        );
        lock.lock();
        try {
            SeckillReservation reservation =
                    reservationRepository.findByOrderId(message.getOrderId());
            // 第二阶段消息必须存在对应的Redis预占记录
            if (reservation == null) {
                throw new IllegalStateException("找不到秒杀订单预占记录，orderId="+ message.getOrderId());
            }
            String status = reservation.getStatus();
            // 库存已经开始补偿或补偿完成，晚到的MQ消息不能再创建订单
            if (SeckillReservation.STATUS_COMPENSATING.equals(status)
                    || SeckillReservation.STATUS_COMPENSATED.equals(status)) {
                LOGGER.warn("订单已经进入补偿流程，忽略晚到消息，orderId={}, status={}",message.getOrderId(),status);
                return;
            }

            // 在同一把订单锁内完成MySQL落库和Redis状态修正
            transactionalService.createOrder(message);
            reservationRepository.markCreated(message.getOrderId());

            LOGGER.info("处理完成RabbitMQ秒杀订单，orderId={}",message.getOrderId());
        } finally {
            lock.unlock();
        }
    }

}
