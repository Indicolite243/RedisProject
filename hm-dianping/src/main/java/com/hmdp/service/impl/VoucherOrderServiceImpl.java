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
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(VoucherOrderServiceImpl.class);

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // ==================== 秒杀下单版本说明 ====================
    // V1：直接查询数据库，判断时间和库存
    // V2：使用synchronized解决单机环境的一人一单
    // V3：使用SimpleRedisLock解决集群环境的一人一单
    // V4：使用Redisson替代自己实现的分布式锁
    // V5：使用Lua脚本判断秒杀资格，再放入阻塞队列异步下单【当前使用】
    // 整理时间：2026-08-18，只整理顺序和版本标记，尽量保留原来的代码和注释
    // ========================================================

    // ==================== V5 阻塞队列和异步消费者（当前使用） ====================
    //请求线程是生产者，把订单放入队列
    //独立线程是消费者，从队列取出订单并保存到数据库
    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    //创建单线程线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //保存当前类的代理对象，异步线程通过代理对象调用事务方法
    private IVoucherOrderService proxy;

    //Spring初始化完成以后，启动消费者线程
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //阻塞队列的消费者
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的订单信息
                    //队列中没有订单时，take()会阻塞等待，不会一直空转
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    //一个订单处理失败，不能让整个消费者线程停止
                    LOGGER.error("处理订单异常", e);
                }
            }
        }
    }

    //异步线程处理订单
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
//        一人一单
        Long userId = voucherOrder.getUserId();

        //使用redisson创建锁对象
        RLock lock = redissonClient.getLock("order:" + userId);

        boolean isLock = lock.tryLock();

        if (!isLock) {
            LOGGER.error("不允许重复下单");
            return;
        }
        try {
//        获取代理对象（事务）
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    // ==================== V5 Lua + 阻塞队列异步下单（当前使用） ====================
    // 优化了什么：库存判断和一人一单在Redis中一次完成，请求线程不再直接操作数据库
    // 当前限制：阻塞队列在JVM内存中，服务宕机时还没消费的订单可能丢失，后面可升级Redis Stream
    @Override
    public Result seckillVoucher(Long voucherId) {
//        ==================== V1 直接查询数据库（已停用） ====================
//        停用原因：秒杀请求会直接访问数据库，高并发时数据库压力大
////        查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
////        判断秒杀是否结束
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
////        判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
////        判断优惠券库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }

//        用lua脚本判断
        Long userId = UserHolder.getUser().getId();
//        1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );

        //【必要修正】Redis异常时result可能为null，不能直接调用intValue()
        if (result == null) {
            return Result.fail("系统繁忙，请稍后重试");
        }

//        2.判断结果为0
        int r = result.intValue();
        if (r!=0){
            //        2.1不为0代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");

        }
//        2.2.为0 有购买资格 把下单信息保存到阻塞队列里
        long orderId = redisIdWorker.nextId("order");

//        创建订单对象
        VoucherOrder voucherOrder = new VoucherOrder();
//        订单id
        voucherOrder.setId(orderId);
//        用户id
        voucherOrder.setUserId(userId);
//        代金券id
        voucherOrder.setVoucherId(voucherId);

//        获取代理对象（事务）
        //【必要调整】必须在请求线程中获取代理对象，异步线程中不能使用AopContext.currentProxy()
        proxy =(IVoucherOrderService)AopContext.currentProxy();

//          保存到阻塞队列，当前请求线程是生产者
        orderTasks.add(voucherOrder);

//        3.返回订单ID
        //这里只代表订单任务已经进入队列，数据库订单由消费者线程异步创建
        return Result.ok(orderId);
    }

    // ==================== V5 异步创建数据库订单（当前使用） ====================
    //【必要调整】异步线程中无法使用UserHolder，所以用户id、优惠券id和订单id都从voucherOrder获取
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {

//        一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

//        查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//                判断是否存在
        if (count > 0) {
            LOGGER.error("不能重复下单");
            return;
        }

        //        扣减库存
        //乐观锁cas法加判断条件 判断秒杀库存前后的值一致
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")//set stock=stock-1
                .eq("voucher_id", voucherId)
                .gt("stock", 0)//where id=? and stock>0 解决命中率低的问题
                .update();

        if (!success){
            LOGGER.error("库存不足");
            return;
        }

//        保存订单
        //订单对象已经在请求线程中设置好了订单id、用户id和代金券id
        save(voucherOrder);
    }

    /*
     * ==================== V2 synchronized版（已停用） ====================
     * 升级了什么：同一个用户的请求串行执行，解决单机环境下的一人一单并发问题
     * 为什么停用：只能锁住当前这一台Java服务，多台服务之间无法互相感知
     *
     * Long userId = UserHolder.getUser().getId();
     * synchronized (userId.toString().intern()) {
     *     //获取代理对象（事务）
     *     IVoucherOrderService porxy =(IVoucherOrderService)AopContext.currentProxy();
     *     //返回订单ID
     *     return porxy.createVoucherOrder(voucherId);
     * }
     *
     * ========================================================================
     * ==================== V3 SimpleRedisLock版（已停用） ====================
     * 升级了什么：使用Redis分布式锁，可以保护多台Java服务
     * 为什么停用：自己实现分布式锁还要处理误删、超时、可重入和自动续期等问题
     *
     * //尝试创建锁对象
     * SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
     * boolean isLock = lock.tryLock(1200L);
     * if (!isLock) {
     *     return Result.fail("不允许重复下单");
     * }
     * try {
     *     IVoucherOrderService porxy =(IVoucherOrderService)AopContext.currentProxy();
     *     return porxy.createVoucherOrder(voucherId);
     * } finally {
     *     lock.unLock();
     * }
     *
     * ========================================================================
     * ==================== V4 Redisson同步下单版（已停用） ====================
     * 升级了什么：Redisson提供可重入、看门狗自动续期和Lua原子释放锁
     * 为什么停用：请求线程还是要同步查询和修改数据库，秒杀性能仍受数据库限制
     *
     * //使用redisson创建锁对象
     * RLock lock = redissonClient.getLock("order:" + userId);
     * boolean isLock = lock.tryLock();
     * if (!isLock) {
     *     return Result.fail("不允许重复下单");
     * }
     * try {
     *     //获取代理对象（事务）
     *     IVoucherOrderService porxy =(IVoucherOrderService)AopContext.currentProxy();
     *     return porxy.createVoucherOrder(voucherId);
     * } finally {
     *     lock.unlock();
     * }
     *
     * V1到V4使用的同步创建订单方法原代码：
     *
     * @Transactional
     * public Result createVoucherOrder(Long voucherId) {
     *     //一人一单
     *     Long userId = UserHolder.getUser().getId();
     *     //查询订单
     *     Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
     *     //判断是否存在
     *     if (count > 0) {
     *         return Result.fail("不能重复下单");
     *     }
     *     //扣减库存
     *     //乐观锁cas法加判断条件 判断秒杀库存前后的值一致
     *     boolean success = seckillVoucherService.update()
     *             .setSql("stock = stock - 1")//set stock=stock-1
     *             .eq("voucher_id", voucherId)
     *             .gt("stock", 0)//where id=? and stock>0 解决命中率低的问题
     *             .update();
     *     if (!success){
     *         return Result.fail("库存不足");
     *     }
     *     //生成订单
     *     VoucherOrder voucherOrder = new VoucherOrder();
     *     //订单id 用户id 代金券id
     *     Long orderId = redisIdWorker.nextId("order");
     *     voucherOrder.setId(orderId);
     *     //用户id
     *     voucherOrder.setUserId(userId);
     *     //代金卷id
     *     voucherOrder.setVoucherId(voucherId);
     *     //保存订单
     *     save(voucherOrder);
     *     //返回订单ID
     *     return Result.ok(orderId);
     * }
     */
}
