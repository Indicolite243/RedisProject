package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */

@Service

public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result seckilllVoucher(Long voucherId) {
//        查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        判断秒杀是否结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
//        判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
//        判断优惠券库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();


//        synchronized (userId.toString().intern()) {
//            //获取代理对象（事务）
//            IVoucherOrderService porxy =(IVoucherOrderService)AopContext.currentProxy();
//            //        返回订单ID
//        return porxy.createVoucherOrder(voucherId);
//        }


//        尝试创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        boolean isLock = lock.tryLock(1200L);

        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
//        获取代理对象（事务）
            IVoucherOrderService porxy =(IVoucherOrderService)AopContext.currentProxy();
            //        返回订单ID
            return porxy.createVoucherOrder(voucherId);
        } finally {
            lock.unLock();
        }
    }

    @Transactional
    public  Result createVoucherOrder(Long voucherId) {

//        一人一单
        Long userId = UserHolder.getUser().getId();

//        查询订单
            Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//                判断是否存在
            if (count > 0) {
                return Result.fail("不能重复下单");
            }

            //        扣减库存
            //乐观锁cas法加判断条件 判断秒杀库存前后的值一致
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")//set stock=stock-1
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)//where id=? and stock>0 解决命中率低的问题
                    .update();

            if (!success){
                return Result.fail("库存不足");
            }

//        生成订单
            VoucherOrder voucherOrder = new VoucherOrder();
//        订单id 用户id 代金券id
            Long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
//        用户id

            voucherOrder.setUserId(userId);
//        代金卷id
            voucherOrder.setVoucherId(voucherId);
//        保存订单
            save(voucherOrder);

            //        返回订单ID
        return Result.ok(orderId);




    }
}
