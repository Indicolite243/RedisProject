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
 * 扫描Redis中的待发布预占记录，并将秒杀订单消息发送到RabbitMQ。
 *
 * <p>一个扫描线程负责按批次读取任务，多个发布线程负责发送消息并等待Publisher Confirm。
 * 这样既避免多个扫描线程反复读取同一批订单，也不会让一条消息的Confirm等待阻塞所有订单。</p>
 */
@Component
@ConditionalOnProperty(
        prefix = "hmdp.seckill",
        name = "reservation-enabled",
        havingValue = "true"
)
public class SeckillMessageDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeckillMessageDispatcher.class);

    private static final int MAX_PUBLISH_ATTEMPTS = 5;
    private static final int PENDING_BATCH_SIZE = 20;
    private static final int PUBLISH_WORKER_COUNT = 4;

    private final SeckillReservationRepository reservationRepository;
    private final ConfirmedSeckillOrderPublisher confirmedOrderPublisher;

    /**
     * 扫描线程始终只有一个，避免多个线程同时扫描同一批Pending订单。
     */
    private final ExecutorService dispatcherExecutor =
            Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "seckill-dispatcher-thread");
                thread.setDaemon(true);
                return thread;
            });

    private final AtomicInteger publisherThreadNumber = new AtomicInteger(1);

    /**
     * 发布线程负责发送RabbitMQ消息并等待Confirm。
     * 某个线程等待确认时，其余线程仍然可以继续发布其他订单。
     */
    private final ExecutorService publishExecutor =
            Executors.newFixedThreadPool(PUBLISH_WORKER_COUNT, runnable -> {
                Thread thread = new Thread(
                        runnable,
                        "seckill-publisher-thread-" + publisherThreadNumber.getAndIncrement()
                );
                thread.setDaemon(true);
                return thread;
            });

    /**
     * 保存当前JVM中已经提交给发布线程的订单ID。
     * Pending记录只有发布成功后才会从Redis删除，因此扫描线程可能重复读到同一订单。
     */
    private final Set<Long> inFlightOrderIds = ConcurrentHashMap.newKeySet();

    public SeckillMessageDispatcher(
            SeckillReservationRepository reservationRepository,
            ConfirmedSeckillOrderPublisher confirmedOrderPublisher
    ) {
        this.reservationRepository = reservationRepository;
        this.confirmedOrderPublisher = confirmedOrderPublisher;
    }

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
     * 持续按发送时间扫描Pending ZSet，并将到期订单交给发布线程池。
     */
    private void dispatchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
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

                    // add返回false说明该订单已经在发布中或正在线程池里等待执行。
                    if (!inFlightOrderIds.add(numericOrderId)) {
                        continue;
                    }

                    try {
                        publishExecutor.submit(() -> publishOneSafely(numericOrderId));
                        submittedCount++;
                    } catch (RejectedExecutionException e) {
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
            // 单个订单的意外异常不能导致发布线程退出。
            LOGGER.error("并发发布秒杀订单消息异常，orderId={}", orderId, e);
        } finally {
            // 失败订单到达下次重试时间后，需要能够再次进入线程池。
            inFlightOrderIds.remove(orderId);
        }
    }

    /**
     * 处理一条到期订单：读取Reservation、发送RabbitMQ，并根据Confirm结果更新状态。
     */
    private void dispatchOne(Long orderId) {
        SeckillReservation reservation = reservationRepository.findByOrderId(orderId);
        if (reservation == null) {
            // ZSet中存在订单ID，但对应的Reservation已经不存在，直接清理无效索引。
            LOGGER.warn("找不到预占记录，orderId={}", orderId);
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
        boolean canPublish = SeckillReservation.STATUS_PREPARED.equals(status)
                || SeckillReservation.STATUS_PUBLISH_RETRY.equals(status);

        if (!canPublish) {
            reservationRepository.removePending(orderId);
            LOGGER.info("订单当前状态不需要继续发布，orderId={}, status={}", orderId, status);
            return;
        }

        SeckillOrderMessageDTO messageDTO = new SeckillOrderMessageDTO(
                reservation.getMessageId(),
                reservation.getOrderId(),
                reservation.getUserId(),
                reservation.getVoucherId(),
                reservation.getCreatedAt(),
                1
        );

        try {
            // 只有收到RabbitMQ的Publisher Confirm，才能标记为PUBLISHED并移出Pending ZSet。
            confirmedOrderPublisher.publishAndAwait(messageDTO);
            reservationRepository.markPublished(orderId);
            LOGGER.info(
                    "秒杀订单消息发布成功，orderId={}, messageId={}",
                    orderId,
                    reservation.getMessageId()
            );
        } catch (Exception e) {
            long nextRetryAt = System.currentTimeMillis() + 3000;
            long result = reservationRepository.scheduleRetry(
                    orderId,
                    nextRetryAt,
                    MAX_PUBLISH_ATTEMPTS
            );

            if (result == 2) {
                LOGGER.error("秒杀订单消息发布达到最大次数，状态已变成DEAD，orderId={}", orderId);
            } else {
                LOGGER.warn("秒杀订单消息发布失败，3秒后重试，orderId={}", orderId, e);
            }
        }
    }
}
