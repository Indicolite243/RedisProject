package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 返回给前端的秒杀订单处理状态。
 *
 * <p>这里暴露的是稳定的业务状态，而不是 PREPARED、PUBLISHED 等内部技术状态，
 * 从而避免前端与 Redis/RabbitMQ 的实现细节耦合。</p>
 */
@Data
@AllArgsConstructor
public class SeckillOrderStatusDTO {

    /** 秒杀接口返回的订单 ID。 */
    private Long orderId;

    /**
     * PROCESSING：仍在发布、消费或补偿处理中；
     * SUCCESS：MySQL 订单已经创建；
     * FAILED：自动处理停止，等待人工重放或补偿；
     * COMPENSATED：Redis 库存和购买资格已经恢复，可以重新抢购。
     */
    private String status;

    /** 面向用户的简短状态说明。 */
    private String message;
}
