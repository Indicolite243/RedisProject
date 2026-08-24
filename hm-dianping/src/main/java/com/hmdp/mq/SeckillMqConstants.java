package com.hmdp.mq;

public final class SeckillMqConstants {

    private SeckillMqConstants() {
    }

    // 正常秒杀订单
    public static final String ORDER_EXCHANGE = "hmdp.seckill.order.direct";
    public static final String ORDER_QUEUE = "hmdp.seckill.order.queue";
    public static final String ORDER_ROUTING_KEY = "order.create";

    // 死信队列
    public static final String DEAD_EXCHANGE = "hmdp.seckill.dead.direct";
    public static final String DEAD_QUEUE = "hmdp.seckill.order.dlq";
    public static final String DEAD_ROUTING_KEY = "order.dead";
}