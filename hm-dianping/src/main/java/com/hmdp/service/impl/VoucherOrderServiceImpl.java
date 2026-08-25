package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderStatusDTO;
import com.hmdp.entity.SeckillReservation;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.repository.SeckillReservationRepository;
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
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Value;
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

    @Resource
    private SeckillReservationRepository reservationRepository;

    /**
     * 旧版 Stream 直连 MySQL 消费者开关。
     * RabbitMQ 链路启用时必须关闭，否则 Stream 消费者与 RabbitMQ Listener
     * 可能同时处理订单，给切换和故障排查带来干扰。
     */
    @Value("${hmdp.seckill.stream-direct-consumer-enabled:true}")
    private boolean streamDirectConsumerEnabled;

    /**
     * 秒杀入口模式开关。
     * true：使用 Reservation Hash + Pending ZSet，随后由 Dispatcher 发布 RabbitMQ；
     * false：使用旧版 seckill.lua，把订单写入 Redis Stream。
     */
    @Value("${hmdp.seckill.reservation-enabled:false}")
    private boolean reservationEnabled;

    /** 旧版 Redis Stream 秒杀脚本，保留用于链路切换和性能对照。 */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    /** 当前 RabbitMQ 链路使用的 Redis 预占脚本。 */
    private static final DefaultRedisScript<Long> SECKILL_RESERVE_V2_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        // 预占脚本会原子完成资格判断、库存预扣、Reservation 写入和 Pending 入队。
        SECKILL_RESERVE_V2_SCRIPT = new DefaultRedisScript<>();
        SECKILL_RESERVE_V2_SCRIPT.setLocation(
                new ClassPathResource("seckill_reserve_v2.lua")
        );
        SECKILL_RESERVE_V2_SCRIPT.setResultType(Long.class);
    }

    // ==================== 秒杀下单版本说明 ====================
    // V1：直接查询数据库，判断时间和库存
    // V2：使用synchronized解决单机环境的一人一单
    // V3：使用SimpleRedisLock解决集群环境的一人一单
    // V4：使用Redisson替代自己实现的分布式锁
    // V5：使用Lua脚本判断秒杀资格，再放入JVM阻塞队列异步下单（已停用）
    // V6：使用Lua脚本判断资格并写入Redis Stream，消费者组异步下单（兼容保留）
    // V7：使用Redis预占记录保存可靠状态，由Dispatcher发布RabbitMQ，Listener异步落库【当前使用】
    // ========================================================

    // ==================== V6 Redis Stream异步消费者（兼容保留） ====================
    // 升级了什么：用Redis Stream替代V5的JVM阻塞队列，消息可以跨服务重启保留
    // 请求线程是生产者：Lua判断资格、预扣Redis库存并把订单消息写入stream.order
    // 独立线程是消费者：从stream.order读取订单，再保存到数据库
    // V6主流程：seckillVoucher -> seckill.lua写入Stream -> VoucherOrderHandler
    //          -> handleVoucherOrder加锁 -> createVoucherOrder事务下单 -> XACK确认

    // 仅供旧 Stream 直连消费者使用；关闭开关后不会向该线程池提交任务。
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //保存当前类的代理对象，异步线程通过代理对象调用事务方法
    private IVoucherOrderService proxy;

    /** Spring 初始化完成后，根据开关决定是否启动旧 Stream 消费者。 */
    @PostConstruct
    private void init() {
        if (!streamDirectConsumerEnabled) {
        LOGGER.info("Redis Stream 直连 MySQL 消费者已关闭");
        return;
        }
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /** 旧版 Redis Stream 消费者，不参与当前 RabbitMQ 主链路。 */
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                String queueName = "stream.order";
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
//                    2.判断消息获取是否成功
                    if (list == null || list.isEmpty()){
//                        2.1如果失败说明没有消息，继续下一次循环
                        continue;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    3.如果成功说明可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 XACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    //一个订单处理失败，不能让整个消费者线程停止
                    LOGGER.error("处理订单异常", e);
                    //处理失败的消息会留在pending-list中，从pending-list重新读取
                    handlePendingList();
                }
            }
        }

        //处理已经被读取，但是还没有ACK确认的消息
        private void handlePendingList()  {
            while (true) {
                String queueName = "stream.order";
                try {
                    //1.获取pending-list的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
//                    2.判断消息获取是否成功
                    if (list == null || list.isEmpty()){
//                        2.1如果没有pending消息，结束本次pending-list处理
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    3.如果成功说明可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 XACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    //一个订单处理失败，不能让整个消费者线程停止
                    LOGGER.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    // ==================== V6 消费订单并调用事务方法（兼容保留） ====================
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

    // ==================== 秒杀请求入口：按配置选择 V6 或 V7 ====================
    // 两种模式都会在请求线程中通过 Lua 原子完成资格判断和 Redis 库存预扣。
    // V6 后续载体是 Redis Stream；V7 后续载体是 Reservation + Pending ZSet + RabbitMQ。
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 请求线程只负责生成订单标识并执行 Redis Lua，不同步等待 MySQL 创建订单。
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        long createdAt = System.currentTimeMillis();// 请求被受理的时间，同时作为首次待发送时间

        String messageId = "seckill-order-" + orderId;

        // 根据开关选择新旧链路。两份脚本都用 0/1/2 表示成功/库存不足/重复下单。
        Long result;
        if (reservationEnabled) {
            // V7 当前链路：保存可恢复的预占状态，并让 Dispatcher 异步发布到 RabbitMQ。
            result = stringRedisTemplate.execute(
                    SECKILL_RESERVE_V2_SCRIPT,//RedisScript 对象
                    Collections.emptyList(),//keys 集合
                    voucherId.toString(),// ARGV[1]
                    userId.toString(),// ARGV[2]
                    String.valueOf(orderId),// ARGV[3]
                    messageId, // ARGV[4]
                    String.valueOf(createdAt)// ARGV[5]
            );
        } else {
            // V6 兼容链路：把订单事件写入 Redis Stream。
            result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString(),
                    String.valueOf(orderId),
                    String.valueOf(createdAt)
            );
        }

        // Redis 执行异常时 result 可能为 null，不能直接调用 intValue()。
        if (result == null) {
            return Result.fail("系统繁忙，请稍后重试");
        }

//        2.判断结果为0
        int r = result.intValue();
        if (r!=0){
            //        2.1不为0代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

//        3.获取代理对象
        proxy =(IVoucherOrderService)AopContext.currentProxy();

        // 返回订单 ID 只代表 Redis 已接受这次秒杀请求，并不代表 MySQL 订单已经创建。
        // 前端可使用该 ID 查询异步状态；RabbitMQ Listener 成功提交事务后才算最终落库。
        return Result.ok(orderId);
    }

    /**
     * 查询秒杀订单的异步处理状态。
     *
     * 查询顺序必须是MySQL在前、Redis在后：MySQL订单是最终业务事实，
     * 即使消费者已经成功落库但Redis状态还没来得及更新，也应该返回成功。
     */
    @Override
    public Result querySeckillOrderStatus(Long orderId, Long currentUserId) {
        if (orderId == null || currentUserId == null) {
            return Result.fail("订单参数错误");
        }

        // 同时使用订单ID和当前用户ID查询，避免用户通过猜测orderId查看别人的订单。
        VoucherOrder databaseOrder = query()
                .eq("id", orderId)
                .eq("user_id", currentUserId)
                .one();

        if (databaseOrder != null) {
            // 数据库已经存在订单就说明业务成功；顺便修正可能滞后的Redis技术状态。
            reservationRepository.markCreated(orderId);
            return statusResult(orderId, "SUCCESS", "订单创建成功");
        }

        // MySQL还没有订单时，再查看Redis中的异步处理进度。
        SeckillReservation reservation = reservationRepository.findByOrderId(orderId);
        if (reservation == null) {
            return Result.fail("订单不存在或处理记录已失效");
        }

        // Reservation中也要校验用户归属，不能仅凭orderId返回技术状态。
        if (reservation.getUserId() == null
                || !currentUserId.equals(reservation.getUserId())) {
            return Result.fail("无权查询该订单");
        }

        String status = reservation.getStatus();

        // 下面四种状态都表示消息仍在正常投递或等待RabbitMQ消费者处理。
        if (SeckillReservation.STATUS_PREPARED.equals(status)
                || SeckillReservation.STATUS_PUBLISHING.equals(status)
                || SeckillReservation.STATUS_PUBLISH_RETRY.equals(status)
                || SeckillReservation.STATUS_PUBLISHED.equals(status)) {
            return statusResult(orderId, "PROCESSING", "订单正在处理中");
        }

        if (SeckillReservation.STATUS_CREATED.equals(status)) {
            return statusResult(orderId, "SUCCESS", "订单创建成功");
        }

        if (SeckillReservation.STATUS_DEAD.equals(status)) {
            return statusResult(orderId, "FAILED", "订单处理失败，等待人工处理");
        }

        // 补偿尚未结束时不能提示用户重新抢购，因此仍然返回处理中。
        if (SeckillReservation.STATUS_COMPENSATING.equals(status)) {
            return statusResult(orderId, "PROCESSING", "订单正在进行库存补偿");
        }

        if (SeckillReservation.STATUS_COMPENSATED.equals(status)) {
            return statusResult(orderId, "COMPENSATED", "订单已补偿，可以重新抢购");
        }

        LOGGER.error("发现未知的秒杀订单技术状态，orderId={}, status={}", orderId, status);
        return Result.fail("订单状态异常，请稍后重试");
    }

    /**
     * 统一构造状态查询成功结果，避免每个状态都重复创建DTO。
     */
    private Result statusResult(Long orderId, String status, String message) {
        return Result.ok(new SeckillOrderStatusDTO(orderId, status, message));
    }

    // ==================== V6 异步创建数据库订单（当前使用） ====================
    // V5和V6都使用这个事务方法：异步线程无法使用UserHolder，所以三个id都从voucherOrder获取
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
        //订单id、用户id和代金券id已经保存在Stream消息中，消费者读取消息后封装成voucherOrder
        save(voucherOrder);
    }

    // ==================== V5 Lua + JVM阻塞队列异步下单（已停用） ====================
    // 升级了什么：库存判断和一人一单在Redis中一次完成，请求线程不再直接操作数据库
    // 为什么停用：阻塞队列在JVM内存中，服务宕机时还没消费的订单会丢失，因此V6升级为Redis Stream
    // 同一批V5代码统一保留在这里，方便和V6对照

//    //请求线程是生产者，把订单放入队列
//    //独立线程是消费者，从队列取出订单并保存到数据库
//    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

//    //阻塞队列的消费者
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    //1.获取队列中的订单信息
//                    //队列中没有订单时，take()会阻塞等待，不会一直空转
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    //一个订单处理失败，不能让整个消费者线程停止
//                    LOGGER.error("处理订单异常", e);
//                }
//            }
//        }
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
////        用lua脚本判断
//        Long userId = UserHolder.getUser().getId();
////        1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(),
//                userId.toString()
//        );
//
//        //【必要修正】Redis异常时result可能为null，不能直接调用intValue()
//        if (result == null) {
//            return Result.fail("系统繁忙，请稍后重试");
//        }
//
////        2.判断结果为0
//        int r = result.intValue();
//        if (r!=0){
//            //        2.1不为0代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
////        2.2.为0 有购买资格 把下单信息保存到阻塞队列里
//        long orderId = redisIdWorker.nextId("order");
//
////        创建订单对象
//        VoucherOrder voucherOrder = new VoucherOrder();
////        订单id
//        voucherOrder.setId(orderId);
////        用户id
//        voucherOrder.setUserId(userId);
////        代金券id
//        voucherOrder.setVoucherId(voucherId);
//
////        获取代理对象（事务）
//        //V5必须在请求线程中获取代理对象，异步线程中不能使用AopContext.currentProxy()
//        proxy =(IVoucherOrderService)AopContext.currentProxy();
//
////          保存到阻塞队列，当前请求线程是生产者
//        orderTasks.add(voucherOrder);
//
////        3.返回订单ID
//        //这里只代表订单任务已经进入队列，数据库订单由消费者线程异步创建
//        return Result.ok(orderId);
//    }

    /*
     * ==================== V1 直接查询数据库版（已停用） ====================
     * 实现了什么：直接查询优惠券时间和库存，再同步创建数据库订单
     * 为什么停用：秒杀请求直接访问数据库，高并发时数据库压力大
     *
     * //查询优惠券
     * SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
     * //判断秒杀是否开始
     * if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
     *     return Result.fail("秒杀尚未开始");
     * }
     * //判断秒杀是否已经结束
     * if (voucher.getEndTime().isBefore(LocalDateTime.now())){
     *     return Result.fail("秒杀已经结束");
     * }
     * //判断优惠券库存是否充足
     * if (voucher.getStock() < 1) {
     *     return Result.fail("库存不足");
     * }
     *
     * ========================================================================
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
