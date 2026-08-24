package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 秒杀订单 MQ 消息。
 *
 * messageId：业务消息唯一标识；同一订单重发时必须保持不变。
 * orderId：雪花算法生成的订单 ID，也是数据库主键。
 * createdAt：秒杀请求被受理的时间戳，用于统计端到端异步延迟。
 * schemaVersion：消息结构版本，后续扩字段时便于兼容。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessageDTO {

    private String messageId;

    private Long orderId;

    private Long userId;

    private Long voucherId;

    private Long createdAt;

    private Integer schemaVersion;
}