package com.hmdp.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis中的秒杀订单预占记录。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillReservation {

    // 消息处理状态
    public static final String STATUS_PREPARED = "PREPARED";
    public static final String STATUS_PUBLISHING = "PUBLISHING";
    public static final String STATUS_PUBLISH_RETRY = "PUBLISH_RETRY";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String STATUS_CREATED = "CREATED";
    public static final String STATUS_DEAD = "DEAD";
    public static final String STATUS_COMPENSATING = "COMPENSATING";
    public static final String STATUS_COMPENSATED = "COMPENSATED";

    private Long orderId;

    private String messageId;

    private Long userId;

    private Long voucherId;

    // 当前处理状态
    private String status;

    // 已经尝试发送RabbitMQ的次数
    private Integer publishAttempts;

    // 下次允许重新发送的时间戳
    private Long nextRetryAt;

    // 秒杀请求受理时间
    private Long createdAt;
}