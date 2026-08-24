package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 查询秒杀订单当前的异步处理状态。
     *
     * 秒杀接口成功只表示请求已经被系统受理，并不代表MySQL订单已经立即创建。
     * 前端可以拿秒杀接口返回的orderId调用本接口，了解后续处理结果。
     */
    @GetMapping("/{orderId}/status")
    public Result querySeckillOrderStatus(@PathVariable("orderId") Long orderId) {
        // 本接口受登录拦截器保护，UserHolder中保存的是当前请求对应的用户。
        Long currentUserId = UserHolder.getUser().getId();
        return voucherOrderService.querySeckillOrderStatus(orderId, currentUserId);
    }
}
