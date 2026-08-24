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
 * DEAD秒杀订单的人工补偿服务。
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
     * 人工补偿一条DEAD订单。
     *
     * @return 1表示补偿成功或此前已经补偿；0表示状态不允许或MySQL已有订单；
     *         -2表示Redis用户订单映射异常；-3表示Redis库存异常
     */
    public long compensate(Long orderId) {
        // 必须与Listener使用完全相同的锁Key。
        RLock lock = redissonClient.getLock(
                "lock:seckill:order:" + orderId
        );

        lock.lock();

        try {
            // 获取锁后重新读取，防止等待锁期间状态发生变化。
            SeckillReservation reservation =
                    reservationRepository.findByOrderId(orderId);

            if (reservation == null) {
                LOGGER.warn("找不到需要补偿的预占记录，orderId={}", orderId);
                return 0L;
            }

            String status = reservation.getStatus();

            // 重复补偿直接视为成功，Lua不会再次增加库存。
            if (SeckillReservation.STATUS_COMPENSATED.equals(status)) {
                return 1L;
            }

            if (!SeckillReservation.STATUS_DEAD.equals(status)) {
                LOGGER.warn(
                        "订单当前状态不允许补偿，orderId={}, status={}",
                        orderId,
                        status
                );
                return 0L;
            }

            // 补偿前必须确认MySQL中没有该用户的优惠券订单。
            VoucherOrder existingOrder = voucherOrderService.query()
                    .eq("user_id", reservation.getUserId())
                    .eq("voucher_id", reservation.getVoucherId())
                    .one();

            if (existingOrder != null) {
                // 同一个orderId说明数据库订单已经创建，只是Redis状态没有更新。
                if (existingOrder.getId().equals(orderId)) {
                    reservationRepository.markCreated(orderId);
                }

                LOGGER.warn(
                        "MySQL已经存在订单，拒绝恢复库存，reservationOrderId={}, databaseOrderId={}",
                        orderId,
                        existingOrder.getId()
                );
                return 0L;
            }

            // MySQL确实没有订单，执行Redis原子补偿。
            long result = reservationRepository.compensate(reservation);

            if (result == 1L) {
                LOGGER.info(
                        "秒杀订单补偿完成，orderId={}, voucherId={}, userId={}",
                        orderId,
                        reservation.getVoucherId(),
                        reservation.getUserId()
                );
            } else {
                LOGGER.error(
                        "秒杀订单补偿失败，orderId={}, result={}",
                        orderId,
                        result
                );
            }

            return result;
        } finally {
            lock.unlock();
        }
    }
}
