package com.hmdp.mq;

import com.hmdp.dto.SeckillOrderMessageDTO;
import com.hmdp.entity.SeckillReservation;
import com.hmdp.mq.repository.SeckillReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis Reservation 到 RabbitMQ 的可靠消息调度器。
 *
 * <p>秒杀接口不会直接调用 RabbitMQ，因为“扣减 Redis 库存”和“发送 MQ”无法放在一个
 * 本地事务中。接口先用 Lua 原子写入 Reservation Hash 与 Pending ZSet，本类随后扫描
 * Pending 并发布消息。这是一种简化的本地消息表思想：Redis 中的记录就是可恢复的发送凭证。</p>
 *
 * <p>线程模型：</p>
 * <ul>
 *     <li>一个扫描线程：按 nextRetryAt 查找已经到期的订单；</li>
 *     <li>四个发布线程：并行发送消息并等待 Publisher Confirm；</li>
 *     <li>一个 JVM 内 inFlight 集合：避免同一订单被本实例重复提交。</li>
 * </ul>
 *
 * <p>即使应用在发布后、更新 PUBLISHED 前崩溃，重启后也会再次发送该订单。
 * 因此该链路保证的是“至少一次发布”，消费端必须幂等。</p>
 */
@Component
@ConditionalOnProperty(
        prefix = "hmdp.seckill",
        name = "reservation-enabled",
        havingValue = "true"
)
public class SeckillMessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeckillMessageDispatcher.class);

    /** 单笔订单允许的最大发布次数，达到后转为 DEAD，停止自动发送。 */
    private static final int MAX_PUBLISH_ATTEMPTS = 5;

    /** 每轮最多从 Pending ZSet 取出的订单数量，避免一次加载过多任务。 */
    private static final int PENDING_BATCH_SIZE = 20;

    /** 并行发布线程数。Confirm 等待是阻塞操作，因此不能只使用一个发布线程。 */
    private static final int PUBLISH_WORKER_COUNT = 4;

    /** 封装 Reservation Hash、Pending ZSet 及 Lua 状态流转。 */
    private final SeckillReservationRepository reservationRepository;

    /** 负责发送消息并同时校验 Confirm 与 Return。 */
    private final ConfirmedSeckillOrderPublisher confirmedOrderPublisher;

    /**
     * 扫描线程始终只有一个，避免本实例内多个线程同时扫描同一批 Pending 订单。
     * 线程设置为 daemon，使 JVM 退出时不会被无限循环阻塞。
     */
    private final ExecutorService dispatcherExecutor =
            Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "seckill-dispatcher-thread");
                thread.setDaemon(true);
                return thread;
            });

    private final AtomicInteger publisherThreadNumber = new AtomicInteger(1);//原子计数器对象publisherThreadNumber

    /**
     * 发布线程负责发送 RabbitMQ 消息并等待 Confirm。
     * 某个线程等待确认时，其余线程仍然可以继续发布其他订单，避免单条慢消息阻塞整批订单。
     */
    private final ExecutorService publishExecutor =
            Executors.newFixedThreadPool(PUBLISH_WORKER_COUNT, runnable -> {//4个阻塞线程
                Thread thread = new Thread(
                        runnable,
                        "seckill-publisher-thread-" + publisherThreadNumber.getAndIncrement()//原子加1
                );
                thread.setDaemon(true);//守护线程，后台线程
                return thread;
            });

    /**
     * 保存当前 JVM 中已经提交给发布线程的订单 ID。
     *
     * <p>Pending 记录只有发布成功后才会从 Redis 删除，扫描线程在发布完成前可能反复读到
     * 同一订单。Set.add() 的原子返回值可保证同一实例只提交一次。这个集合不是持久化状态，
     * 应用重启后清空并没有问题，因为 Redis Pending 才是真正的恢复依据。</p>
     */
    private final Set<Long> inFlightOrderIds = ConcurrentHashMap.newKeySet();

    public SeckillMessageDispatcher(
            SeckillReservationRepository reservationRepository,
            ConfirmedSeckillOrderPublisher confirmedOrderPublisher
    ) {
        this.reservationRepository = reservationRepository;
        this.confirmedOrderPublisher = confirmedOrderPublisher;
    }

    /**
     * Spring 完成依赖注入后启动后台扫描线程。
     * @PostConstruct 本身运行在启动线程中，因此这里只提交任务，不直接执行无限循环。
     */
    @PostConstruct
    private void init() {
        dispatcherExecutor.submit(this::dispatchLoop);
    }

    @PreDestroy
    private void destroy() {
        // 中断后台任务，避免应用关闭后继续访问正在销毁的Redis和RabbitMQ连接。
        dispatcherExecutor.shutdownNow();
        publishExecutor.shutdownNow();
    }

    /**
     * 持续按发送时间扫描 Pending ZSet，并将到期订单交给发布线程池。
     *
     * <p>ZSet score 是 nextRetryAt，只查询 score <= now 的成员，所以失败订单在重试时间
     * 到达前不会被高频重复发送。</p>
     */
    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // score 范围使用当前毫秒时间戳，只领取已经到达发送时间的任务。
                long now = System.currentTimeMillis();
                Set<String> orderIds = reservationRepository.findPendingOrders(now, PENDING_BATCH_SIZE);

                if (orderIds.isEmpty()) {
                    // 没有待发送订单时短暂等待，兼顾Redis压力和新订单的发布延迟。
                    Thread.sleep(50);
                    continue;
                }

                int submittedCount = 0;
                for (String orderId : orderIds) {
                    Long numericOrderId = Long.valueOf(orderId);

                    // add 返回 false，说明该订单已经在发布中或正在线程池里等待执行。
                    if (!inFlightOrderIds.add(numericOrderId)) {//inFlightOrderIds在途订单
                        continue;
                    }

                    try {
                        // 扫描线程只负责分发，网络发送和 Confirm 等待交给发布线程。
                        publishExecutor.submit(() -> publishOneSafely(numericOrderId));
                        submittedCount++;//记录已发送数
                    } catch (RejectedExecutionException e) {
                        // 线程池拒绝任务时必须撤销 inFlight 标记，否则订单会永远无法再次提交。
                        inFlightOrderIds.remove(numericOrderId);
                        if (publishExecutor.isShutdown()) {
                            return;
                        }
                        throw e;
                    }
                }

                if (submittedCount == 0) {
                    // 当前批次都在处理中，避免扫描线程高频空转查询Redis。
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOGGER.error("秒杀订单调度异常", e);
            }
        }
    }

    private void publishOneSafely(Long orderId) {
        try {
            dispatchOne(orderId);
        } catch (Exception e) {
            // dispatchOne 已处理正常的发布失败；这里兜底捕获状态解析等意外异常，
            // 防止单个坏数据导致发布工作线程退出。
            LOGGER.error("并发发布秒杀订单消息异常，orderId={}", orderId, e);
        } finally {
            // 失败订单到达下次重试时间后，需要能够再次进入线程池。
            inFlightOrderIds.remove(orderId);
        }
    }

    /**
     * 处理一条到期订单：读取 Reservation、发送 RabbitMQ，并根据 Confirm 结果更新状态。
     *
     * @param orderId Pending ZSet 中取出的订单 ID
     */
    private void dispatchOne(Long orderId) {
        SeckillReservation reservation = reservationRepository.findByOrderId(orderId);
        if (reservation == null) {
            // ZSet 中存在订单 ID，但对应 Reservation 已不存在，说明它是孤立索引。
            // 清理索引，避免扫描线程永远重复读取同一个无效任务。
            LOGGER.warn("找不到预占记录，orderId={}", orderId);
            // 非可发布状态仍残留在 Pending 中时进行自修复，避免无意义扫描。
            reservationRepository.removePending(orderId);
            return;
        }

        /*
         * 状态                 是否发送    说明
         * PREPARED             是          首次发送
         * PUBLISH_RETRY        是          发布失败后重试
         * PUBLISHING           否          当前流程暂未使用
         * PUBLISHED            否          已经进入RabbitMQ
         * CREATED              否          MySQL订单已经创建
         * DEAD                 否          等待后续补偿
         * COMPENSATING         否          正在执行补偿
         * COMPENSATED          否          补偿已经完成
         */
        String status = reservation.getStatus();
        //prepared和publish_retry才可以发布
        boolean canPublish = SeckillReservation.STATUS_PREPARED.equals(status)
                || SeckillReservation.STATUS_PUBLISH_RETRY.equals(status);

        if (!canPublish) {
            reservationRepository.removePending(orderId);
            LOGGER.info("订单当前状态不需要继续发布，orderId={}, status={}", orderId, status);
            return;
        }

        // DTO 完全由持久化的 Reservation 重建。重试时 messageId 和业务字段保持不变，
        // schemaVersion=1 表示当前消息结构的第一版。
        SeckillOrderMessageDTO messageDTO = new SeckillOrderMessageDTO(
                reservation.getMessageId(),
                reservation.getOrderId(),
                reservation.getUserId(),
                reservation.getVoucherId(),
                reservation.getCreatedAt(),
                1
        );

        try {
            // 只有 Confirm ACK 且消息未被 Return，才能标记为 PUBLISHED 并移出 Pending。
            // 先发送、后改状态会留下一个短暂重复窗口，但不会丢消息；重复由消费者幂等处理。
            confirmedOrderPublisher.publishAndAwait(messageDTO);
            reservationRepository.markPublished(orderId);
            LOGGER.info(
                    "秒杀订单消息发布成功，orderId={}, messageId={}",
                    orderId,
                    reservation.getMessageId()
            );
        } catch (Exception e) {
            // 发布失败不立即自旋重试，而是把下次发送时间设置到 3 秒后。
            // Lua 同时增加 publishAttempts，并在达到上限时原子转为 DEAD。
            long nextRetryAt = System.currentTimeMillis() + 3000;
            long result = reservationRepository.scheduleRetry(
                    orderId,
                    nextRetryAt,
                    MAX_PUBLISH_ATTEMPTS//5次机会
            );

            if (result == 2) {
                LOGGER.error("秒杀订单消息发布达到最大次数，状态已变成DEAD，orderId={}", orderId);
            } else {
                LOGGER.warn("秒杀订单消息发布失败，3秒后重试，orderId={}", orderId, e);
            }
        }
    }
}
