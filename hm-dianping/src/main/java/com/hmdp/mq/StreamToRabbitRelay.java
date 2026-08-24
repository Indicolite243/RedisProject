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

//桥接器 桥接stream和rabbitmq
@Component
@ConditionalOnProperty(//只有配置文件中的指定属性满足条件时，Spring 才创建这个 Bean。
        prefix = "hmdp.mq",
        name = "stream-relay-enabled",
        havingValue = "true"
)
public class StreamToRabbitRelay {
    /** Lua 写入秒杀订单消息的 Redis Stream。 */
    private static final String STREAM_KEY = "stream.order";
    /**
     * 沿用旧 Stream 消费者使用的消费者组。
     * 这样 Relay 可以从当前消费者组的消费进度继续处理订单。
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
     * 多实例部署时，每个实例必须配置不同且稳定的消费者名称。
     */
    @Value("${hmdp.mq.stream-relay-consumer-name:relay-1}")
    private String consumerName;

    private static final Logger LOGGER =LoggerFactory.getLogger(StreamToRabbitRelay.class);

    //开启独立线程
    private final ExecutorService relayExecutor =Executors.newSingleThreadExecutor(runnable->{
        Thread thread = new Thread(
                runnable,
                "stream-relay-thread"
        );
        thread.setDaemon( true);
        return thread;
    });
    // Spring 初始化完成后启动 Relay
    @PostConstruct
    private void init() {
        relayExecutor.submit(new StreamRelayHandler());
    }

    // Stream 转发任务
//    启动 Relay 线程
//          ↓
//    循环读取 stream.order
//       ↓
//    有消息吗？
//        ┌──┴──┐
//        没有   有
//        ↓      ↓
//    继续等  取出第一条消息
//        ↓
//    后续转换成 DTO
//        ↓
//    发送到 RabbitMQ
//        ↓
//    RabbitMQ 确认成功后 XACK
    private class StreamRelayHandler implements Runnable {

        @Override
        public void run() {
            // 应用重启后，优先恢复relay-1上次未完成的消息
            handlePendingList();
            while (true) {
                try {
                    // 以g1组中当前Relay消费者的身份读取新消息，每次最多取一条并阻塞两秒。
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),//表示每次最多读取一条消息，没有消息时，Redis 最多等待两秒：
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())//指定从哪个 Stream、哪个位置读取，ReadOffset.lastConsumed()消费者组尚未分配的新消息
                    );
                    // 阻塞时间结束后仍然没有消息，继续下一轮读取
                    if (list == null || list.isEmpty()) {
                        // 没有消息时，继续等待
                        continue;
                    }
                    // COUNT 1，所以只取第一条消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    LOGGER.info("收到消息：record:{},value;{}", record.getId(), record.getValue());
                    // 将Stream记录转换成DTO，可靠发布到RabbitMQ后再确认该记录。
                    relayRecord(record);
                } catch (Exception e) {
                    LOGGER.error("Relay转发Stream消息异常", e);

                    // 发布失败的消息没有XACK，进入Pending处理
                    handlePendingList();
                }
            }

        }

        //复用逻辑
        private void relayRecord(MapRecord<String, Object, Object> record) throws Exception {
            // 取得Stream消息中的字段
            Map<Object, Object> value = record.getValue();
            Long orderId = Long.valueOf(value.get("id").toString());
            Long userId = Long.valueOf(value.get("userId").toString());
            Long voucherId = Long.valueOf(value.get("voucherId").toString());
            Long createdAt = Long.valueOf(value.get("createdAt").toString());

            // 2将Stream消息转换为RabbitMQ消息
            SeckillOrderMessageDTO message = new SeckillOrderMessageDTO(
                    "seckill-order-" + orderId,
                    orderId,
                    userId,
                    voucherId,
                    createdAt,
                    1
            );
            // 将订单消息可靠发送到RabbitMQ，成功后再执行XACK。
            // 只有Confirm ACK并且消息没有被Return，方法才会正常结束
            publisher.publishAndAwait(message);
            // RabbitMQ发布成功后，确认Redis Stream消息
            stringRedisTemplate.opsForStream().acknowledge(
                    STREAM_KEY,
                    GROUP_NAME,
                    record.getId()
            );
            LOGGER.info("消息转换完成：record={},message={}", record.getId(), message);
        }

        // 处理PendingList
        private void handlePendingList() {
            while (true) {
                try {
                    // 读取relay-1自己尚未ACK的Pending消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, consumerName),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );

                    //pending已经处理完毕，结束循环
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    // 复用相同的发布和ACK逻辑
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
