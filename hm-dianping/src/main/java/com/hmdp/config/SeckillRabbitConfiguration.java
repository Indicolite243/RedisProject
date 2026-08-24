package com.hmdp.config;


import com.hmdp.mq.SeckillMqConstants;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SeckillRabbitConfiguration {
    //秒杀订单交换机
    @Bean
    public DirectExchange seckillOrderExchange(){
        return new DirectExchange(
                SeckillMqConstants.ORDER_EXCHANGE,
                true,
                false);
    }
    //秒杀订单队列
    @Bean
    public Queue seckillOrderQueue(){
        return QueueBuilder.durable(SeckillMqConstants.ORDER_QUEUE)
                .deadLetterExchange(SeckillMqConstants.DEAD_EXCHANGE)
                .deadLetterRoutingKey(SeckillMqConstants.DEAD_ROUTING_KEY)
                .build();
    }
    //秒杀订单队列绑定交换机
    @Bean
    public Binding seckillOrderBinding(){
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(SeckillMqConstants.ORDER_ROUTING_KEY);
    }
    // 死信交换机
    @Bean
    public DirectExchange seckillDeadExchange() {
        return new DirectExchange(
                SeckillMqConstants.DEAD_EXCHANGE,
                true,
                false
        );
    }
    //死信队列
    @Bean
    public Queue seckillDeadQueue(){
        return QueueBuilder.durable(SeckillMqConstants.DEAD_QUEUE).build();
    }
    //死信队列绑定交换机
    @Bean
    public Binding seckillDeadBinding(){
        return BindingBuilder.bind(seckillDeadQueue())
                .to(seckillDeadExchange())
                .with(SeckillMqConstants.DEAD_ROUTING_KEY);
    }
    //消息转换器
    @Bean
    public MessageConverter rabbitMessageConverter(){
        return new Jackson2JsonMessageConverter();
    }

    /**
     * 秒杀订单消息流（第一阶段）：
     *
     * 当前阶段：
     *
     *  应用启动
     *      │
     *      ├── 声明 hmdp.seckill.order.direct        主交换机
     *      ├── 声明 hmdp.seckill.order.queue         主队列
     *      ├── 声明 hmdp.seckill.dead.direct         死信交换机（DLX）
     *      └── 声明 hmdp.seckill.order.dlq           死信队列（DLQ）
     *
     * 后续完整链路：
     *
     *  HTTP 秒杀请求
     *      │
     *      ▼
     *  Redis Lua：预扣库存 + XADD Stream
     *      │
     *      ▼
     *  Stream Relay
     *      │  routingKey = order.create
     *      ▼
     *  hmdp.seckill.order.direct
     *      │
     *      ▼
     *  hmdp.seckill.order.queue
     *      │
     *      ▼
     *  @RabbitListener：幂等校验 + MySQL 事务落库
     *      │
     *      ├── 成功：AUTO ACK，消息消失
     *      │
     *      └── 失败：Spring Retry 重试 3 次
     *                 │
     *                 ▼
     *             reject + requeue=false
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
     * 当前还没有 Producer 和 Listener。
     * 启动时主动建立一次 AMQP 连接，触发 RabbitAdmin 声明本类中的
     * Exchange、Queue 与 Binding。
     */
    @Bean
    public ApplicationRunner rabbitTopologyInitializer(RabbitTemplate rabbitTemplate) {
        return args -> rabbitTemplate.execute(channel -> null);
    }



}
