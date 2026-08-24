package com.hmdp.service;

import com.hmdp.dto.SeckillOrderMessageDTO;

public interface IVoucherOrderTransactionalService {

//    幂等创建秒杀订单
    void createOrder(SeckillOrderMessageDTO message);
}
