package com.hmdp.service.impl;

import com.hmdp.dto.SeckillOrderMessageDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherOrderTransactionalService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * RabbitMQ 消费端的 MySQL 事务实现。
 *
 * <p>Redis 预扣库存解决入口高并发，MySQL 仍然保存最终订单和最终库存。
 * 本类把两次数据库写操作放进一个事务，防止出现“有订单但没扣库存”或
 * “扣了库存但没有订单”的中间状态。</p>
 */
@Service
public class VoucherOrderTransactionalServiceImpl implements IVoucherOrderTransactionalService {

    /** 操作 tb_voucher_order 订单表。 */
    private final IVoucherOrderService voucherOrderService;

    /** 操作 tb_seckill_voucher 秒杀库存表。 */
    private final ISeckillVoucherService seckillVoucherService;

    public VoucherOrderTransactionalServiceImpl(IVoucherOrderService voucherOrderService, ISeckillVoucherService seckillVoucherService) {
        this.voucherOrderService = voucherOrderService;
        this.seckillVoucherService = seckillVoucherService;
    }

    @Override
    @Transactional
    public void createOrder(SeckillOrderMessageDTO message) {
        // RabbitMQ 可能重复投递，因此先按“用户 + 优惠券”查询业务订单。
        // 已存在就把本次消费视为幂等成功，Listener 随后仍会修正 Reservation 状态。
        VoucherOrder existingOrder = voucherOrderService.query()
                .eq("user_id", message.getUserId())
                .eq("voucher_id", message.getVoucherId())
                .one();

        if (existingOrder!=null){
            // 同一用户已经存在该优惠券订单，不再重复创建，也不能再次扣减 MySQL 库存。
            return;
        }

        // 将 MQ 消息转换成数据库订单实体。orderId 在 HTTP 请求阶段已经生成，
        // 消费端必须沿用该 ID，不能重新生成，否则无法与 Reservation 对应。
        VoucherOrder order = new VoucherOrder();
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());
        order.setId(message.getOrderId());

        // 先保存订单。如果后面的条件扣库存失败，@Transactional 会把本次 INSERT 一并回滚。
        boolean saved = voucherOrderService.save(order);
        if (!saved){
            // 抛出运行时异常，使 MySQL 事务回滚，并让 RabbitMQ Listener 触发重试。
            throw new IllegalStateException("订单保存失败");
        }

        // 使用“stock > 0”作为 SQL 更新条件，数据库层再次防止库存扣成负数：
        // UPDATE tb_seckill_voucher
        // SET stock = stock - 1
        // WHERE voucher_id = ? AND stock > 0;
        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", message.getVoucherId())
                .gt("stock", 0)// Greater Than：只允许扣减仍有库存的记录
                .update();
        if (!stockUpdated){
            // 受影响行数为 0，说明 MySQL 库存不足或券不存在。
            // 抛异常后订单 INSERT 也会回滚，消息随后进入重试流程。
            throw new IllegalStateException("库存不足");
        }



    }
}
