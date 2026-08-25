package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 秒杀订单 MQ 消息契约。
 *
 * <p>这个 DTO 是 Dispatcher/Relay 与 RabbitMQ Listener 之间的数据边界。
 * 重试发送同一订单时，各业务字段必须保持不变，消费者才能把重复投递识别为同一笔业务。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessageDTO {

    /**
     * 业务消息唯一标识。同一订单无论发送多少次都保持不变，
     * 与每次物理投递都会变化的 Publisher correlationId 不同。
     */
    private String messageId;

    /** RedisIdWorker 生成的订单 ID，同时作为 MySQL 订单主键。 */
    private Long orderId;

    /** 发起秒杀请求的用户 ID，用于一人一单和订单归属校验。 */
    private Long userId;

    /** 被抢购的秒杀券 ID。 */
    private Long voucherId;

    /** HTTP 秒杀请求被 Redis 接受的毫秒时间戳，可用于计算端到端处理延迟。 */
    private Long createdAt;

    /** 消息结构版本。后续增加或调整字段时，消费者可据此选择兼容逻辑。 */
    private Integer schemaVersion;
}
