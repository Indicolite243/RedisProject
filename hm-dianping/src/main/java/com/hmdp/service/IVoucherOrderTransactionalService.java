package com.hmdp.service;

import com.hmdp.dto.SeckillOrderMessageDTO;

/**
 * RabbitMQ 消费端的订单落库事务边界。
 *
 * <p>单独抽成 Spring Service，是为了让 {@code @Transactional} 通过代理生效，
 * 保证“保存订单”和“扣减 MySQL 库存”要么同时成功，要么同时回滚。</p>
 */
public interface IVoucherOrderTransactionalService {

    /**
     * 幂等创建秒杀订单。
     *
     * @param message RabbitMQ 中反序列化得到的订单消息
     * @throws RuntimeException 保存订单或扣减库存失败时抛出，让 Listener 触发消息重试
     */
    void createOrder(SeckillOrderMessageDTO message);
}
