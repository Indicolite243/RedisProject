package com.hmdp.mq;

import com.hmdp.dto.SeckillOrderMessageDTO;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis Stream 到 RabbitMQ 的迁移桥接器。
 *
 * <p>它只用于改造过渡期：旧版 Lua 仍把订单写入 {@code stream.order} 时，Relay
 * 以消费者组成员的身份读取 Stream，再将同一订单可靠发布到 RabbitMQ。</p>
 *
 * <p>关键可靠性规则是：只有 {@link ConfirmedSeckillOrderPublisher} 确认消息已进入
 * RabbitMQ 且没有被 Return，才能对 Stream 记录执行 XACK。发布失败时不 XACK，
 * 记录会留在 Pending List，应用恢复后可以再次处理。</p>
 *
 * <p>当前 Redis Reservation + Pending ZSet + Dispatcher 链路不再依赖本类，
 * 正式切换后应保持 {@code hmdp.mq.stream-relay-enabled=false}，避免两条链路并行发布。</p>
 */
@Component
@ConditionalOnProperty(
        prefix = "hmdp.mq",
        name = "stream-relay-enabled",
        havingValue = "true"
)
public class StreamToRabbitRelay {
    /** 旧版 {@code seckill.lua} 写入秒杀订单消息的 Redis Stream。 */
    private static final String STREAM_KEY = "stream.order";
    /**
     * 沿用旧 Stream 消费者使用的消费者组，使 Relay 能从该组已有的消费进度继续处理。
     */
    private static final String GROUP_NAME = "g1";

    private final StringRedisTemplate stringRedisTemplate;
    private final ConfirmedSeckillOrderPublisher publisher;

    public StreamToRabbitRelay(StringRedisTemplate stringRedisTemplate, ConfirmedSeckillOrderPublisher publisher) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.publisher = publisher;
    }
    /**
     * Relay 在消费者组中的消费者名称。
     *
     * 默认使用 relay-1，不再使用旧消费者名称 c1。
     * 多实例部署时，每个实例必须配置不同且稳定的消费者名称，否则 Pending 归属会混乱。
     */
    @Value("${hmdp.mq.stream-relay-consumer-name:relay-1}")
    private String consumerName;

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamToRabbitRelay.class);

    /**
     * Relay 使用独立单线程保持读取、发布、XACK 的处理顺序。
     * 守护线程不会阻止 JVM 正常退出。
     */
    private final ExecutorService relayExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(
                runnable,
                "stream-relay-thread"
        );
        thread.setDaemon(true);
        return thread;
    });
    /** Spring 创建完 Bean 后提交后台转发任务。 */
    @PostConstruct
    private void init() {
        relayExecutor.submit(new StreamRelayHandler());
    }

    /** Stream 消费与转发任务。 */
    private class StreamRelayHandler implements Runnable {

        @Override
        public void run() {
            // 应用重启后先恢复当前 consumer 上次未完成的 Pending 消息，再读取新消息。
            handlePendingList();
            while (true) {
                try {
                    // 以 g1 组中当前 Relay consumer 的身份读取“尚未分配给组内消费者”的新消息。
                    // count(1) 控制单次处理一条；block(2s) 避免无消息时持续空轮询占用 CPU。
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );
                    // 阻塞时间结束后仍然没有消息，继续下一轮读取。
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    // COUNT 为 1，所以只处理返回列表中的第一条记录。
                    MapRecord<String, Object, Object> record = list.get(0);
                    LOGGER.info("收到消息：record:{},value;{}", record.getId(), record.getValue());
                    // 转换成 DTO 并可靠发布；relayRecord 内部只有成功发布后才会 XACK。
                    relayRecord(record);
                } catch (Exception e) {
                    LOGGER.error("Relay转发Stream消息异常", e);

                    // 发布失败的消息没有 XACK，会保留在 Pending List 中等待恢复。
                    handlePendingList();
                }
            }

        }

        /**
         * 转发一条 Stream 记录，并在 RabbitMQ 可靠发布成功后确认原记录。
         */
        private void relayRecord(MapRecord<String, Object, Object> record) throws Exception {
            // 读取旧 Stream 消息字段。项目不兼容缺少 createdAt 的更早期历史消息。
            Map<Object, Object> value = record.getValue();
            Long orderId = Long.valueOf(value.get("id").toString());
            Long userId = Long.valueOf(value.get("userId").toString());
            Long voucherId = Long.valueOf(value.get("voucherId").toString());
            Long createdAt = Long.valueOf(value.get("createdAt").toString());

            // 将 Stream 数据转换成 RabbitMQ 统一消息契约。
            SeckillOrderMessageDTO message = new SeckillOrderMessageDTO(
                    "seckill-order-" + orderId,
                    orderId,
                    userId,
                    voucherId,
                    createdAt,
                    1
            );
            // 只有收到 Confirm ACK 且消息没有被 Return，方法才会正常返回。
            publisher.publishAndAwait(message);
            // RabbitMQ 已可靠接收后再 XACK，防止“先确认 Stream，后发布失败”造成订单丢失。
            stringRedisTemplate.opsForStream().acknowledge(
                    STREAM_KEY,
                    GROUP_NAME,
                    record.getId()
            );
            LOGGER.info("消息转换完成：record={},message={}", record.getId(), message);
        }

        /**
         * 恢复当前 Relay consumer 名下尚未 XACK 的消息。
         *
         * <p>{@code ReadOffset.from("0")} 在消费者组读取语义中表示读取当前消费者自己的
         * Pending 消息，而不是从整个 Stream 的第一条记录重新消费。</p>
         */
        private void handlePendingList() {
            while (true) {
                try {
                    // 读取当前 consumer 自己尚未 ACK 的 Pending 消息。
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );

                    // 当前 consumer 的 Pending 已处理完毕，回到新消息读取循环。
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    // Pending 与新消息复用完全相同的发布和 ACK 顺序。
                    relayRecord(record);

                } catch (Exception e) {
                    LOGGER.error("处理PendingList异常", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }


}
