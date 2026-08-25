package com.hmdp.mq;

import com.hmdp.entity.SeckillReservation;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.repository.SeckillReservationRepository;
import com.hmdp.service.IVoucherOrderService;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * DEAD 秒杀订单的人工补偿服务。
 *
 * <p>当消息连续发布失败达到上限时，Reservation 会进入 DEAD。此时 Redis 中已经
 * 扣过库存并记录了“一人一单”，但 MySQL 订单可能尚未创建，不能直接机械地恢复库存。</p>
 *
 * <p>补偿流程会先使用与 RabbitMQ Listener 相同的订单锁串行化处理，再查询 MySQL
 * 这个最终事实来源。只有确认数据库中不存在订单时，才调用 Lua 原子恢复 Redis 库存
 * 和用户购买资格。这样可以避免“订单已经落库，却又把资格和库存退回”的超卖风险。</p>
 */
@Service
public class SeckillReservationCompensationService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SeckillReservationCompensationService.class);

    private final SeckillReservationRepository reservationRepository;
    private final IVoucherOrderService voucherOrderService;
    private final RedissonClient redissonClient;

    public SeckillReservationCompensationService(
            SeckillReservationRepository reservationRepository,
            IVoucherOrderService voucherOrderService,
            RedissonClient redissonClient) {
        this.reservationRepository = reservationRepository;
        this.voucherOrderService = voucherOrderService;
        this.redissonClient = redissonClient;
    }

    /**
     * 人工补偿一条 DEAD 订单。
     *
     * @return 1 表示补偿成功或此前已经补偿；0 表示状态不允许或 MySQL 已有订单；
     *         -2 表示 Redis 用户订单映射异常；-3 表示 Redis 库存异常
     */
    public long compensate(Long orderId) {
        // 必须与 Listener 使用完全相同的锁 Key。
        // 这样“消费者正在落库”和“管理员正在补偿”不会同时处理同一笔订单。
        RLock lock = redissonClient.getLock("lock:seckill:order:" + orderId);

        lock.lock();

        try {
            // 获取锁后重新读取，防止等待锁期间 Reservation 已被 Listener 或其他补偿请求修改。
            SeckillReservation reservation =reservationRepository.findByOrderId(orderId);
            if (reservation == null) {
                LOGGER.warn("找不到需要补偿的预占记录，orderId={}", orderId);
                return 0L;
            }

            String status = reservation.getStatus();
            // 重复补偿直接视为成功，保证人工接口具备幂等性；Lua 也不会再次增加库存。
            if (SeckillReservation.STATUS_COMPENSATED.equals(status)) {
                return 1L;
            }
//            补偿机制
//            RabbitMQ 已成功投递，但消费者落库失败 3 次
//            → 消息进入 DLQ
//            → Reservation 通常仍是 PUBLISHED
//            → 当前没有自动补偿，也不能直接 compensate()

            //发布到 RabbitMQ 失败 5 次
            //→ Reservation = DEAD
            //→ 可以人工调用 compensate(orderId)
            if (!SeckillReservation.STATUS_DEAD.equals(status)) {
                LOGGER.warn("订单当前状态不允许补偿，orderId={}, status={}",orderId,status);
                return 0L;
            }

            // 补偿前必须确认 MySQL 中没有该用户的优惠券订单。
            // 查询 userId + voucherId，而不只查 orderId，是为了守住“一人一单”的业务约束。
            VoucherOrder existingOrder = voucherOrderService.query()
                    .eq("user_id", reservation.getUserId())
                    .eq("voucher_id", reservation.getVoucherId())
                    .one();

            if (existingOrder != null) {
                // 相同 orderId 说明数据库订单已经创建，只是 Redis 状态没有及时更新。
                // 此时补记 CREATED，让 Redis 状态与 MySQL 最终结果重新一致。
                if (existingOrder.getId().equals(orderId)) {
                    reservationRepository.markCreated(orderId);
                }
                LOGGER.warn("MySQL已经存在订单，拒绝恢复库存，reservationOrderId={}, databaseOrderId={}",orderId,existingOrder.getId());
                return 0L;
            }

            // MySQL 确实没有订单，才执行 Redis 原子补偿。
            long result = reservationRepository.compensate(reservation);
            if (result == 1L) {
                LOGGER.info("秒杀订单补偿完成，orderId={}, voucherId={}, userId={}",orderId,reservation.getVoucherId(),reservation.getUserId());
            } else {
                LOGGER.error("秒杀订单补偿失败，orderId={}, result={}",orderId,result);
            }
            return result;
        } finally {
            lock.unlock();
        }
    }
}
