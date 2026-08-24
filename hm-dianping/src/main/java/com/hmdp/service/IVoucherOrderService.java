package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 查询秒杀订单的异步处理状态。
     *
     * @param orderId       秒杀接口返回的订单ID
     * @param currentUserId 当前登录用户ID，用于防止查询其他用户的订单
     * @return PROCESSING、SUCCESS、FAILED或COMPENSATED
     */
    Result querySeckillOrderStatus(Long orderId, Long currentUserId);

    void createVoucherOrder(VoucherOrder voucherOrder);
}
