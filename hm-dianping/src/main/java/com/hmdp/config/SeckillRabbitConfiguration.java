package com.hmdp.config;


import com.hmdp.mq.SeckillMqConstants;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 秒杀订单 RabbitMQ 拓扑配置。
 *
 * <p>本类只负责声明消息基础设施，不负责发送或消费消息。Spring Boot 启动时会把这里的
 * {@link Exchange}、{@link Queue} 和 {@link Binding} Bean 交给 RabbitAdmin，在 Broker
 * 中创建对应的交换机、队列和绑定关系。</p>
 *
 * <p>主链路：order.direct -> order.queue -> SeckillOrderListener。</p>
 * <p>异常链路：主队列消费重试耗尽 -> dead.direct -> order.dlq。</p>
 */
@Configuration
public class SeckillRabbitConfiguration {
    /**
     * 声明秒杀订单主交换机。
     *
     * <p>使用 DirectExchange，是因为当前只有一种明确的订单创建路由键，不需要
     * TopicExchange 的通配符匹配能力。durable=true 表示 Broker 重启后仍保留交换机，
     * autoDelete=false 表示没有绑定或连接时也不会自动删除。</p>
     */
    @Bean
    public DirectExchange seckillOrderExchange(){
        return new DirectExchange(
                SeckillMqConstants.ORDER_EXCHANGE,
                true,
                false);
    }
    /**
     * 声明秒杀订单主队列，并为它配置死信出口。
     *
     * <p>当 Listener 抛出异常、Spring Retry 重试耗尽，并且配置为不重新入队时，
     * RabbitMQ 会根据这里的 DLX 和死信路由键把消息转入死信队列，保留人工排查依据。</p>
     */
    @Bean
    public Queue seckillOrderQueue(){
        return QueueBuilder.durable(SeckillMqConstants.ORDER_QUEUE)
                .deadLetterExchange(SeckillMqConstants.DEAD_EXCHANGE)
                .deadLetterRoutingKey(SeckillMqConstants.DEAD_ROUTING_KEY)
                .build();
    }
    /**
     * 使用 order.create 路由键把主交换机与主队列绑定。
     * Publisher 必须使用完全相同的路由键，否则消息会触发 Return，而不会进入该队列。
     */
    @Bean
    public Binding seckillOrderBinding(){
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(SeckillMqConstants.ORDER_ROUTING_KEY);
    }
    /** 声明专门接收失败订单消息的持久化死信交换机。 */
    @Bean
    public DirectExchange seckillDeadExchange() {
        return new DirectExchange(
                SeckillMqConstants.DEAD_EXCHANGE,
                true,
                false
        );
    }
    /**
     * 声明持久化死信队列。
     *
     * <p>该队列故意不配置普通消费者，避免问题消息一进入 DLQ 就被自动确认。
     * 运维或开发人员确认原因后，再决定人工重放还是执行库存补偿。</p>
     */
    @Bean
    public Queue seckillDeadQueue(){
        return QueueBuilder.durable(SeckillMqConstants.DEAD_QUEUE).build();
    }
    /** 使用 order.dead 路由键绑定死信交换机和死信队列。 */
    @Bean
    public Binding seckillDeadBinding(){
        return BindingBuilder.bind(seckillDeadQueue())
                .to(seckillDeadExchange())
                .with(SeckillMqConstants.DEAD_ROUTING_KEY);
    }
    /**
     * 使用 Jackson 在 Java DTO 与 JSON 消息体之间转换。
     * Producer 发送 SeckillOrderMessageDTO，Listener 可以直接接收同一 DTO 类型。
     */
    @Bean
    public MessageConverter rabbitMessageConverter(){
        return new Jackson2JsonMessageConverter();
    }

    /**
     * 秒杀订单完整消息流：
     *
     *  应用启动
     *      │
     *      ├── 声明 hmdp.seckill.order.direct        主交换机
     *      ├── 声明 hmdp.seckill.order.queue         主队列
     *      ├── 声明 hmdp.seckill.dead.direct         死信交换机（DLX）
     *      └── 声明 hmdp.seckill.order.dlq           死信队列（DLQ）
     *
     *  HTTP 秒杀请求
     *      │
     *      ▼
     *  Redis Lua：资格判断 + 预扣库存 + Reservation + Pending ZSet
     *      │
     *      ▼
     *  Dispatcher：扫描 Pending + Publisher Confirm
     *      │  routingKey = order.create
     *      ▼
     *  hmdp.seckill.order.direct
     *      │
     *      ▼
     *  hmdp.seckill.order.queue
     *      │
     *      ▼
     *  @RabbitListener：订单锁 + 幂等校验 + MySQL 事务落库
     *      │
     *      ├── 成功：AUTO ACK，消息消失
     *      │
     *      └── 失败：Spring Retry 重试 3 次
     *                 │
     *                 ▼
     *             reject，且 requeue=false
     *                 │
     *                 ▼
     *  hmdp.seckill.dead.direct
     *      │  routingKey = order.dead
     *      ▼
     *  hmdp.seckill.order.dlq
     *
     *  注意：DLQ 不配置普通 Listener，避免失败消息被自动确认、丢失排查依据。
     */


    /**
     * 应用启动时主动建立一次 AMQP 连接。
     *
     * <p>RabbitAdmin 只有在连接 Broker 后才能真正声明交换机、队列和绑定。
     * 主动连接可以让拓扑错误在启动阶段尽早暴露，而不是等第一条订单消息发送时才发现。</p>
     */
    @Bean
    public ApplicationRunner rabbitTopologyInitializer(RabbitTemplate rabbitTemplate) {
        return args -> rabbitTemplate.execute(channel -> null);
    }



}
