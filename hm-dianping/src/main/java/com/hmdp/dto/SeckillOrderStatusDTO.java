package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SeckillOrderStatusDTO {

    private Long orderId;

    /**
     * PROCESSING：处理中
     * SUCCESS：订单创建成功
     * FAILED：处理失败，等待人工处理
     * COMPENSATED：已补偿，可以重新抢购
     */
    private String status;

    private String message;
}