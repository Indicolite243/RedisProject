package com.hmdp.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis 中的秒杀订单预占记录。
 *
 * <p>秒杀 Lua 扣减 Redis 库存后，会先创建该 Hash，再把 orderId 加入 Pending ZSet。
 * 因此即使应用在发送 RabbitMQ 前崩溃，Dispatcher 重启后仍能从 Redis 找回待发送订单。</p>
 *
 * <p>它不是数据库订单，而是异步链路的技术状态记录。MySQL 中的 VoucherOrder
 * 才是最终业务事实。</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillReservation {

    /** Redis 已完成预占，等待 Dispatcher 首次发布。 */
    public static final String STATUS_PREPARED = "PREPARED";

    /** 预留状态，表示发布动作正在执行；当前 Dispatcher 尚未显式写入该状态。 */
    public static final String STATUS_PUBLISHING = "PUBLISHING";

    /** 上次发布失败，等待 nextRetryAt 到期后重试。 */
    public static final String STATUS_PUBLISH_RETRY = "PUBLISH_RETRY";

    /** Broker 已 Confirm ACK 且消息未被 Return，说明消息已经进入目标队列。 */
    public static final String STATUS_PUBLISHED = "PUBLISHED";

    /** RabbitMQ 消费者已经成功创建 MySQL 订单。 */
    public static final String STATUS_CREATED = "CREATED";

    /** 发布达到最大次数，自动投递停止，等待人工处置。 */
    public static final String STATUS_DEAD = "DEAD";

    /** 人工补偿已经开始，晚到的 MQ 消息不能再创建订单。 */
    public static final String STATUS_COMPENSATING = "COMPENSATING";

    /** 库存和用户购买资格均已恢复，补偿闭环完成。 */
    public static final String STATUS_COMPENSATED = "COMPENSATED";

    /** 订单 ID，也是 Redis Reservation Key 的后缀。 */
    private Long orderId;

    /** 稳定的业务消息 ID，同一订单重试发布时保持不变。 */
    private String messageId;

    /** 预占库存的用户 ID。 */
    private Long userId;

    /** 被预占库存的秒杀券 ID。 */
    private Long voucherId;

    /** 当前内部技术状态，取值见本类 STATUS_* 常量。 */
    private String status;

    /** 已经尝试发布到 RabbitMQ 的次数，达到上限后进入 DEAD。 */
    private Integer publishAttempts;

    /** 下次允许重新发布的毫秒时间戳，同时作为 Pending ZSet 的 score。 */
    private Long nextRetryAt;

    /** Redis 成功受理秒杀请求的毫秒时间戳。 */
    private Long createdAt;
}
