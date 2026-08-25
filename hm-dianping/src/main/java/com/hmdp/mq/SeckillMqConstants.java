package com.hmdp.mq;

/**
 * 秒杀订单 RabbitMQ 拓扑名称。
 *
 * <p>Producer、拓扑配置和 Listener 必须共享同一组常量，避免交换机、队列或路由键
 * 因手写字符串不一致而导致消息无法路由。</p>
 */
public final class SeckillMqConstants {

    private SeckillMqConstants() {
    }

    /** 秒杀订单主交换机：接收正常的订单创建消息。 */
    public static final String ORDER_EXCHANGE = "hmdp.seckill.order.direct";

    /** 秒杀订单主队列：由 SeckillOrderListener 消费。 */
    public static final String ORDER_QUEUE = "hmdp.seckill.order.queue";

    /** 主路由键：把订单创建消息从主交换机路由到主队列。 */
    public static final String ORDER_ROUTING_KEY = "order.create";

    /** 死信交换机：接收主队列重试耗尽后被拒绝的消息。 */
    public static final String DEAD_EXCHANGE = "hmdp.seckill.dead.direct";

    /** 死信队列：保留无法自动处理的订单消息，等待人工处置。 */
    public static final String DEAD_QUEUE = "hmdp.seckill.order.dlq";

    /** 死信路由键：连接死信交换机和死信队列。 */
    public static final String DEAD_ROUTING_KEY = "order.dead";
}
