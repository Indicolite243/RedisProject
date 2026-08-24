package com.hmdp.service.impl;

import com.hmdp.dto.SeckillOrderMessageDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherOrderTransactionalService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class VoucherOrderTransactionalServiceImpl implements IVoucherOrderTransactionalService {
    // 操作tb_voucher_order订单表
    private final IVoucherOrderService voucherOrderService;
    // 操作tb_seckill_voucher秒杀库存表
    private final ISeckillVoucherService seckillVoucherService;

    public VoucherOrderTransactionalServiceImpl(IVoucherOrderService voucherOrderService, ISeckillVoucherService seckillVoucherService) {
        this.voucherOrderService = voucherOrderService;
        this.seckillVoucherService = seckillVoucherService;
    }

    @Override
    @Transactional
    public void createOrder(SeckillOrderMessageDTO message) {
        // RabbitMQ可能重复投递，因此先检查该用户是否已有该优惠券订单
        VoucherOrder existingOrder = voucherOrderService.query()
                .eq("user_id", message.getUserId())
                .eq("voucher_id", message.getVoucherId())
                .one();

        if (existingOrder!=null){
            // 同一用户已经存在该优惠券订单，不再重复创建
            return;
        }

        // 将MQ消息转换成数据库订单实体
        VoucherOrder order = new VoucherOrder();
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());
        order.setId(message.getOrderId());

        // 保存订单
        boolean saved = voucherOrderService.save(order);
        if (!saved){
            // 抛出异常，使MySQL事务回滚并触发RabbitMQ重试
            throw new IllegalStateException("订单保存失败");
        }

        // 扣减MySQL中的秒杀库存
        //UPDATE tb_seckill_voucher
        //SET stock = stock - 1
        //WHERE voucher_id = ?
        //  AND stock > 0;
        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", message.getVoucherId())
                .gt("stock", 0)//Greater Than大于
                .update();
        if (!stockUpdated){
            // 抛出异常，使MySQL事务回滚并触发RabbitMQ重试
            throw new IllegalStateException("库存不足");
        }



    }
}
