package com.hmdp.mq;


import com.hmdp.dto.SeckillOrderMessageDTO;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
/**
 * 带 Publisher Confirm 和 Return 校验的秒杀订单发布器。
 *
 * <p>RabbitTemplate.convertAndSend() 返回，只能说明客户端已经执行了发送动作，不能证明
 * Broker 接收成功，更不能证明消息成功路由到了队列。本类把两个确认条件封装在一起：</p>
 *
 * <ol>
 *     <li>Confirm ACK：Broker 已接收并处理本次发布；</li>
 *     <li>没有 ReturnedMessage：交换机按路由键找到了目标队列。</li>
 * </ol>
 *
 * <p>只有两项都满足，Dispatcher 才可以把 Redis Reservation 标记为 PUBLISHED。</p>
 */
@Component
public class ConfirmedSeckillOrderPublisher {

    /** Spring AMQP 提供的消息发送模板，连接、序列化和 Confirm 都由它管理。 */
    private final RabbitTemplate rabbitTemplate;

    /** 使用构造器注入，保证发布器创建时 RabbitTemplate 一定可用。 */
    public ConfirmedSeckillOrderPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * 可靠发布一条秒杀订单消息，并同步等待 Broker 的确认结果。
     *
     * <p>执行顺序：</p>
     * <ol>
     *     <li>为本次物理投递创建唯一 correlationId；</li>
     *     <li>把 DTO 发送到主交换机和主路由键；</li>
     *     <li>等待最多 5 秒 Publisher Confirm；</li>
     *     <li>检查消息是否因为无法路由而被 Return；</li>
     *     <li>任何一步失败都抛异常，由 Dispatcher 安排重试。</li>
     * </ol>
     *
     * @param payload 由 Redis Reservation 还原出的稳定业务消息
     * @throws Exception Confirm 超时、Broker NACK 或消息无法路由时抛出
     */
    public void publishAndAwait(SeckillOrderMessageDTO payload) throws Exception{
        // messageId 是稳定的业务幂等 ID；同一订单重试时不变。
        // correlationId 标识一次具体的物理发送；每次重试都重新生成，便于对应 Confirm。
        String correlationId = payload.getMessageId() + ":" + UUID.randomUUID();

        // CorrelationData 同时保存 Confirm Future 和可能出现的 ReturnedMessage。
        CorrelationData correlationData = new CorrelationData(correlationId);

        rabbitTemplate.convertAndSend(
                SeckillMqConstants.ORDER_EXCHANGE,//指定交换机
                SeckillMqConstants.ORDER_ROUTING_KEY,// 路由键：把订单创建消息从主交换机路由到主队列
                payload,//载荷
                message -> {
                    // 消息持久化要求队列本身也必须是 durable。
                    // 它能提高 Broker 重启后的消息保留能力，但不能替代 Publisher Confirm。
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);

                    // AMQP messageId 使用稳定业务 ID，方便日志追踪和重复消息识别。
                    message.getMessageProperties().setMessageId(payload.getMessageId());
                    return message;
                },
                correlationData
        );

        // Confirm 本来是异步回调，这里通过 Future 最多等待 5 秒，将结果转换为清晰的
        // 成功/异常返回值，便于 Dispatcher 编写顺序化的重试逻辑。
        CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);

        // ACK=false 可能是交换机不存在等 Broker 侧发布失败；超时则表示结果未知。
        // “结果未知”也必须按失败处理并重试，因此消费者必须具备幂等能力。
        if (confirm==null||!confirm.isAck()){
                String reason=confirm==null?"Comfirm 超时":confirm.getReason();
                throw new Exception("RabbitMQ 发布失败："+reason);
        }

        // Confirm ACK 只证明交换机接收了消息。如果路由键没有匹配任何队列，mandatory=true
        // 会让 Broker Return 该消息，因此还要额外检查 returnedMessage。
        Message returnedMessage = correlationData.getReturnedMessage();
        if (returnedMessage!=null){
            throw new Exception("消息无法路由到目标队列,messageId="+payload.getMessageId());
        }
    }



}
