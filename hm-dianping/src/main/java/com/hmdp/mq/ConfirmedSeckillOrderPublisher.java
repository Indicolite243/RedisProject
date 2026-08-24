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
//可靠发布器
@Component
public class ConfirmedSeckillOrderPublisher {
    private final RabbitTemplate rabbitTemplate;
    //构造器注入法 注入rabbitTemplate
    public ConfirmedSeckillOrderPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }
//    把一条秒杀订单消息可靠地投递到 RabbitMQ
//    并明确判断“Broker 收到了且成功路由到了目标队列”。

//    1. 为本次物理投递创建 correlationId
//    2. 发送消息到主交换机 + 路由键
//    3. 等待最多 5 秒 Publisher Confirm
//    4. Confirm 非 ACK：抛异常
//    5. 消息被 Return：抛异常
//    6. 两项都通过：方法正常结束
    public void publishAndAwait(SeckillOrderMessageDTO payload) throws Exception{
        // messageId 是稳定的业务幂等 ID 是唯一的
        // correlationId 是每次物理发送都不同的投递 ID。
        String correlationId = payload.getMessageId() + ":" + UUID.randomUUID();

        CorrelationData correlationData = new CorrelationData(correlationId);

        rabbitTemplate.convertAndSend(
                SeckillMqConstants.ORDER_EXCHANGE,
                SeckillMqConstants.ORDER_ROUTING_KEY,
                payload,
                message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    message.getMessageProperties().setMessageId(payload.getMessageId());
                    return message;
                },
                correlationData
        );
//        获取当前消息对应的 Confirm 异步结果
//        最多等待 5 秒：
//        - 5 秒内收到确认：继续执行；
//        - 5 秒内没有收到确认：抛出 TimeoutException；
//        - 收到确认后，返回 CorrelationData.Confirm 对象。
        CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);

        if (confirm==null||!confirm.isAck()){
                String reason=confirm==null?"Comfirm 超时":confirm.getReason();
                throw new Exception("RabbitMQ 发布失败："+reason);
        }

        Message returnedMessage = correlationData.getReturnedMessage();
        if (returnedMessage!=null){
            throw new Exception("消息无法路由到目标队列,messageId="+payload.getMessageId());
        }
    }



}
