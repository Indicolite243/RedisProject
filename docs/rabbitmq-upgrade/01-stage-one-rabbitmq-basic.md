# 黑马点评 RabbitMQ 改造第一阶段：安全接入、消费扩容与失败隔离

## 1. 阶段目标与边界

第一阶段的目标不是立刻删除 Redis Stream，而是在不降低当前入口可靠性的前提下完成 RabbitMQ 主消费链路：

```text
HTTP 秒杀请求
  → Lua 原子校验、预扣 Redis 库存、XADD Stream（暂时保留）
  → StreamToRabbitRelay 读取 Stream
  → RabbitMQ Publisher Confirm/Return
  → Confirm 成功后 XACK Stream
  → RabbitMQ 多消费者并行落库
  → AUTO ACK
  → 有限重试
  → 重试耗尽进入 DLQ
```

这样设计的原因：如果第一阶段直接删除 `XADD`，改成“Lua 成功后 Java 裸发 RabbitMQ”，会出现 Redis 已扣库存、应用却在发消息前宕机的空窗期。第一阶段让 Stream 暂时充当可靠投递日志，RabbitMQ 成为真正的业务消费队列；第二阶段再用 Redis 预占状态机和待投递 ZSet 替换 Stream。

第一阶段完成后应具备：

- `@RabbitListener` 消费方式；
- 4～8 个消费者并行落库；
- `AUTO ACK`；
- 1 秒、2 秒、4 秒退避重试；
- 重试耗尽进入 DLQ；
- 数据库唯一索引保障一人一单；
- RabbitMQ 重复消息不会产生重复订单；
- Stream 仍可作为过渡期安全网和回滚入口。

第一阶段明确不解决：

- 完全移除 Redis Stream；
- Redis 预占状态机；
- 定时投递对账与自动库存补偿；
- RabbitMQ、Redis、MySQL 三者的全局强事务。

> 第一阶段可以用于功能、扩容、重试和 DLQ 验证；最终切换到无 Stream 架构必须完成第二阶段。

---

## 2. 技术环境总确认

### 2.1 当前项目版本

实施前确认以下基线，不要直接照搬 Spring Boot 2.7/3.x 教程 API：

| 组件 | 当前项目版本/建议 |
|---|---|
| Spring Boot | `2.3.12.RELEASE` |
| Spring AMQP | 由 Boot 管理为 `2.2.18.RELEASE` |
| Spring Retry | 由 Boot 管理为 `1.2.5.RELEASE` |
| Java 源码级别 | Java 8 |
| 当前本机运行 JDK | JDK 21，需要保留现有 Lombok 兼容配置 |
| Redis | 当前项目服务器；Stream 消费组必须存在 |
| RabbitMQ | 建议 3.8+，第一阶段可使用单节点开发环境 |
| MySQL | 当前 `hmdp` 数据库 |
| JMeter | 5.6.x，使用非 GUI 模式压测 |

当前版本兼容性注意：

- Spring AMQP 2.2 使用 `CorrelationData#getFuture()`，类型是 `ListenableFuture`；
- Return 信息通过 `CorrelationData#getReturnedMessage()` 获取；
- 不使用新版本的 `ReturnsCallback`、`CompletableFuture` 示例；
- 当前版本没有 `RepublishMessageRecovererWithConfirms`，它是更高版本才加入的能力；
- 第一阶段使用 Broker DLX 路由死信，避免依赖新版本 Recoverer。

### 2.2 基础设施检查命令

```powershell
# Java 与 Maven
java -version
mvn -version

# 项目编译；先不执行当前会写 Redis 的 @SpringBootTest
mvn -DskipTests package

# RabbitMQ TCP 端口
Test-NetConnection -ComputerName <rabbit-host> -Port 5672

# RabbitMQ 管理台（如启用 management 插件）
Test-NetConnection -ComputerName <rabbit-host> -Port 15672

# Redis
Test-NetConnection -ComputerName <redis-host> -Port 6379

# MySQL
Test-NetConnection -ComputerName <mysql-host> -Port 3306
```

### 2.3 实施前备份与分支

建议建立独立分支：

```powershell
git switch -c codex/rabbitmq-stage1
git status --short
```

实施前记录：

- 当前 Git commit；
- Redis、RabbitMQ、MySQL 地址与版本；
- 当前秒杀券 ID、Redis 库存和数据库库存；
- 当前 `stream.order` 长度、Pending 数量；
- 当前 JMeter 基线结果。

不要把真实密码写入 Git；建议通过环境变量覆盖配置。

---

## 3. 数据库唯一索引

### 3.1 技术环境确认

执行 DDL 前确认：

```sql
USE hmdp;
SHOW CREATE TABLE tb_voucher_order;
SHOW INDEX FROM tb_voucher_order;
```

检查当前是否已有重复订单：

```sql
SELECT user_id, voucher_id, COUNT(*) AS order_count
FROM tb_voucher_order
GROUP BY user_id, voucher_id
HAVING COUNT(*) > 1;
```

预期结果为空。如果存在重复数据，先人工确认测试数据和真实数据，不允许直接批量删除。

### 3.2 建立唯一索引

对已经存在的数据库执行：

```sql
ALTER TABLE tb_voucher_order
ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id);
```

同时修改项目初始化 SQL：

> 下方只是表结构差异示意，包含“省略原有字段”，不能整段复制执行或覆盖现有建表语句。真正可执行的线上变更只有前面的 `ALTER TABLE`；初始化脚本应在项目原始完整 `CREATE TABLE` 中追加唯一键。

```sql
CREATE TABLE `tb_voucher_order` (
  `id` bigint(20) NOT NULL COMMENT '主键',
  `user_id` bigint(20) UNSIGNED NOT NULL COMMENT '下单用户id',
  `voucher_id` bigint(20) UNSIGNED NOT NULL COMMENT '优惠券id',
  -- 省略原有字段
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uk_user_voucher` (`user_id`, `voucher_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

为什么必须有两层约束：

```text
PRIMARY KEY(id)
  → 防止相同 orderId 重复落库

UNIQUE(user_id, voucher_id)
  → 防止同一用户用不同 orderId 重复购买同一优惠券
```

验证：

```sql
SHOW INDEX FROM tb_voucher_order;
```

应看到 `PRIMARY` 和 `uk_user_voucher`。

---

## 4. 引入 RabbitMQ 依赖与配置

### 4.1 技术环境确认

确认当前 `pom.xml` 仍由 Spring Boot Parent 管理版本。不要手工指定 Spring AMQP 4.x：

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>2.3.12.RELEASE</version>
</parent>
```

确认 RabbitMQ 中使用独立虚拟主机和非 guest 远程用户，例如：

```text
Virtual Host: /hmdp
User: hmdp
Permissions: configure/write/read on /hmdp
```

### 4.2 Maven 依赖

在 `pom.xml` 增加：

```xml
<!-- Spring AMQP；版本由 Spring Boot 2.3.12 管理为 2.2.18.RELEASE -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

### 4.3 application.yaml

```yaml
hmdp:
  mq:
    # 第一阶段总开关，便于停用 Rabbit 消费链路并恢复原 Stream 消费者
    enabled: true
    # 过渡期 Stream -> Rabbit 转发器
    stream-relay-enabled: true
    # Rabbit Listener 是否启动；回滚时可以关闭
    listener-enabled: true

spring:
  rabbitmq:
    host: ${HMDP_RABBIT_HOST:127.0.0.1}
    port: ${HMDP_RABBIT_PORT:5672}
    virtual-host: ${HMDP_RABBIT_VHOST:/hmdp}
    username: ${HMDP_RABBIT_USERNAME:hmdp}
    password: ${HMDP_RABBIT_PASSWORD:change-me}
    connection-timeout: 2s

    # 第一阶段 Relay 只有确认 Rabbit 收到且未被 Return，才能 XACK Stream
    publisher-confirm-type: correlated
    publisher-returns: true
    template:
      mandatory: true

    listener:
      simple:
        acknowledge-mode: auto
        concurrency: 4
        max-concurrency: 8
        prefetch: 10
        # 重试耗尽后 Reject，不回主队列；由队列 DLX 路由到 DLQ
        default-requeue-rejected: false
        retry:
          enabled: true
          initial-interval: 1s
          multiplier: 2
          max-interval: 4s
          max-attempts: 3
          # 当前监听方法本身不维护有状态会话；每次数据库调用是独立事务
          stateless: true
```

这里的 Publisher Confirm 位于后台 Stream Relay，不阻塞 HTTP 请求线程。第一阶段可靠模式必须开启 Confirm；如果关闭，就不能安全判断何时 XACK Stream。

---

## 5. RabbitMQ 拓扑和消息模型

### 5.1 技术环境确认

确认 RabbitMQ 中不存在同名但参数不同的旧队列。RabbitMQ 不允许用不同参数重新声明同名队列，否则会报 `PRECONDITION_FAILED`。

如已存在测试队列，先通过管理台确认这些参数：

- durable；
- dead-letter-exchange；
- dead-letter-routing-key；
- queue type。

不要在程序启动时盲目删除生产队列。

### 5.2 常量

```java
package com.hmdp.mq;

public final class SeckillMqConstants {
    private SeckillMqConstants() {
    }

    public static final String ORDER_EXCHANGE = "hmdp.seckill.order.direct";
    public static final String ORDER_QUEUE = "hmdp.seckill.order.queue";
    public static final String ORDER_ROUTING_KEY = "order.create";

    public static final String DEAD_EXCHANGE = "hmdp.seckill.dead.direct";
    public static final String DEAD_QUEUE = "hmdp.seckill.order.dlq";
    public static final String DEAD_ROUTING_KEY = "order.dead";
}
```

### 5.3 拓扑配置

```java
package com.hmdp.config;

import com.hmdp.mq.SeckillMqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.MessageConverter;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SeckillRabbitConfiguration {

    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(SeckillMqConstants.ORDER_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(SeckillMqConstants.ORDER_QUEUE)
                // 本地重试耗尽后，Reject 的消息由 Broker 转入 DLQ
                .deadLetterExchange(SeckillMqConstants.DEAD_EXCHANGE)
                .deadLetterRoutingKey(SeckillMqConstants.DEAD_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(SeckillMqConstants.ORDER_ROUTING_KEY);
    }

    @Bean
    public DirectExchange seckillDeadExchange() {
        return new DirectExchange(SeckillMqConstants.DEAD_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillDeadQueue() {
        return QueueBuilder.durable(SeckillMqConstants.DEAD_QUEUE).build();
    }

    @Bean
    public Binding seckillDeadBinding() {
        return BindingBuilder.bind(seckillDeadQueue())
                .to(seckillDeadExchange())
                .with(SeckillMqConstants.DEAD_ROUTING_KEY);
    }

    @Bean
    public MessageConverter rabbitMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
```

### 5.4 消息 DTO

不要直接把数据库实体当作长期消息协议：

```java
package com.hmdp.mq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderMessage {
    /** 业务消息ID；重发时保持不变。 */
    private String messageId;
    /** 订单ID，同时是数据库主键和业务幂等键。 */
    private Long orderId;
    private Long userId;
    private Long voucherId;
    /** 创建时间戳，用于计算端到端落库延迟。 */
    private Long createdAt;
    /** 消息结构版本，未来增加字段时用于兼容。 */
    private Integer schemaVersion;
}
```

第一阶段可以按 Stream record 的 `orderId` 生成：

```java
String messageId = "seckill-order-" + orderId;
```

重投时不得生成新的 `orderId` 或 `messageId`。

为了正确统计“HTTP受理 → MySQL落库”的端到端延迟，第一阶段应把请求时间一起写入 Stream，而不是在 Relay 中重新生成：

```lua
-- 新增ARGV[4]：HTTP请求执行Lua时的毫秒时间戳
local createdAt = ARGV[4]

redis.call('xadd', 'stream.order', '*',
    'userId', userId,
    'voucherId', voucherId,
    'id', id,
    'createdAt', createdAt
)
```

Java 执行 Lua 时传入同一个时间戳。Relay 读取历史消息时如果没有 `createdAt`，可以临时使用 Relay 当前时间，但该类历史消息不能纳入严格的端到端延迟报告。

---

## 6. Stream 到 RabbitMQ 的过渡 Relay

### 6.1 技术环境确认

确认当前 Stream 和消费者组：

```redis
XINFO STREAM stream.order
XINFO GROUPS stream.order
XPENDING stream.order g1
```

要求：

- `stream.order` 存在；
- `g1` 存在；
- 切换前记录 Pending 数量；
- 先停止旧的“Stream 直接落库消费者”，再启动 Relay，避免两个消费者同时争用相同消息语义；
- Relay 使用独立 consumer name，例如 `relay-{实例ID}`，不能继续写死 `c1`。

### 6.2 可靠发布器

以下代码兼容 Spring AMQP 2.2 的 `ListenableFuture` API：

```java
package com.hmdp.mq;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.UUID;

@Component
public class ConfirmedSeckillOrderPublisher {
    private final RabbitTemplate rabbitTemplate;

    public ConfirmedSeckillOrderPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * Relay 后台线程使用同步等待 Confirm；不会阻塞 HTTP 请求线程。
     * 只有 ack=true 且消息没有被 Return，才认为已经进入目标队列。
     */
    public void publishAndAwait(SeckillOrderMessage payload) throws Exception {
        // 业务messageId重发时保持不变；CorrelationData id必须对每次物理发送唯一，
        // 避免上一次Confirm晚到时与本次发送冲突。
        String correlationId = payload.getMessageId() + ":" + UUID.randomUUID();
        CorrelationData correlation = new CorrelationData(correlationId);

        rabbitTemplate.convertAndSend(
                SeckillMqConstants.ORDER_EXCHANGE,
                SeckillMqConstants.ORDER_ROUTING_KEY,
                payload,
                message -> {
                    // 明确设置持久消息，并把业务ID放入Header方便排障。
                    message.getMessageProperties()
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    message.getMessageProperties()
                            .setMessageId(payload.getMessageId());
                    return message;
                },
                correlation
        );

        CorrelationData.Confirm confirm = correlation.getFuture().get(5, TimeUnit.SECONDS);
        if (confirm == null || !confirm.isAck()) {
            String reason = confirm == null ? "confirm timeout" : confirm.getReason();
            throw new IllegalStateException("RabbitMQ NACK: " + reason);
        }

        // Spring AMQP 2.2 保证 returnedMessage 在 confirm future 完成前写入。
        Message returned = correlation.getReturnedMessage();
        if (returned != null) {
            throw new IllegalStateException("消息无法路由到队列: " + payload.getMessageId());
        }
    }
}
```

### 6.3 Relay 核心处理规则

Relay 可以复用当前 `XREADGROUP` 代码，但职责从“创建数据库订单”改为“发布 RabbitMQ 消息”：

```java
private void relayRecord(MapRecord<String, Object, Object> record) throws Exception {
    Map<Object, Object> value = record.getValue();

    Long orderId = Long.valueOf(value.get("id").toString());
    Long userId = Long.valueOf(value.get("userId").toString());
    Long voucherId = Long.valueOf(value.get("voucherId").toString());

    Object createdAtValue = value.get("createdAt");
    long createdAt = createdAtValue == null
            ? System.currentTimeMillis()
            : Long.parseLong(createdAtValue.toString());

    SeckillOrderMessage message = new SeckillOrderMessage(
            "seckill-order-" + orderId,
            orderId,
            userId,
            voucherId,
            createdAt,
            1
    );

    // 只有 Rabbit ACK 且未 Return 才会正常返回。
    publisher.publishAndAwait(message);

    // 发布可靠成功后才确认 Stream，避免 Rabbit 故障时丢掉入口消息。
    stringRedisTemplate.opsForStream().acknowledge(
            "stream.order", "g1", record.getId()
    );
}
```

异常处理原则：

```java
try {
    relayRecord(record);
} catch (Exception e) {
    log.error("Stream转发RabbitMQ失败, recordId={}", record.getId(), e);
    // 不XACK，消息继续留在Pending；避免20ms热循环，使用有上限退避。
    Thread.sleep(1000L);
}
```

必须补充的改进：

- consumer name 使用应用实例 ID；
- 服务启动先处理自己的 Pending，再读 `>` 新消息；
- 多实例恢复其他已死亡 consumer 的 Pending，后续可使用 `XAUTOCLAIM`；
- 第一阶段也可只运行单 Relay，因为 RabbitMQ 消费端才是主要扩容点；
- Relay 的重复发布是允许的，消费者必须幂等。

---

## 7. RabbitMQ 消费者与数据库事务

### 7.1 技术环境确认

确认：

- 唯一索引已经建立；
- `ORDER_QUEUE` 能看到 Ready/Unacked 指标；
- MySQL 连接池最大连接数不少于 Rabbit 消费并发数；
- 旧 Stream 数据库消费者已停止；
- `@Transactional` 方法放在独立 Spring Bean 中，避免 `this` 调用和 `AopContext.currentProxy()` 初始化竞态。

### 7.2 Listener

```java
package com.hmdp.mq;

import com.hmdp.service.impl.VoucherOrderTransactionalService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "hmdp.mq.listener-enabled",
        havingValue = "true",
        matchIfMissing = false
)
public class SeckillOrderListener {
    private final VoucherOrderTransactionalService orderService;

    public SeckillOrderListener(VoucherOrderTransactionalService orderService) {
        this.orderService = orderService;
    }

    @RabbitListener(queues = SeckillMqConstants.ORDER_QUEUE)
    public void onMessage(SeckillOrderMessage message) {
        validate(message);

        // 正常返回：Spring AUTO ACK。
        // 抛出异常：Spring Retry在消费者本地有限重试。
        orderService.createOrderIdempotently(message);
    }

    private void validate(SeckillOrderMessage message) {
        if (message == null
                || message.getOrderId() == null
                || message.getUserId() == null
                || message.getVoucherId() == null
                || message.getSchemaVersion() == null
                || message.getSchemaVersion() != 1) {
            throw new IllegalArgumentException("非法秒杀订单消息");
        }
    }
}
```

不要这样写：

```java
try {
    orderService.createOrderIdempotently(message);
} catch (Exception e) {
    log.error("消费失败", e);
    // 错误：吞掉异常后方法正常返回，AUTO模式会ACK，消息永久删除。
}
```

### 7.3 独立事务服务

```java
package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.SeckillOrderMessage;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class VoucherOrderTransactionalService {
    private final IVoucherOrderService voucherOrderService;
    private final ISeckillVoucherService seckillVoucherService;

    public VoucherOrderTransactionalService(
            IVoucherOrderService voucherOrderService,
            ISeckillVoucherService seckillVoucherService) {
        this.voucherOrderService = voucherOrderService;
        this.seckillVoucherService = seckillVoucherService;
    }

    @Transactional
    public void createOrderIdempotently(SeckillOrderMessage message) {
        // 快速幂等判断；最终正确性仍由数据库唯一索引保证。
        VoucherOrder sameId = voucherOrderService.getById(message.getOrderId());
        if (sameId != null) {
            verifySameOrder(sameId, message);
            return;
        }

        VoucherOrder sameBusiness = voucherOrderService.query()
                .eq("user_id", message.getUserId())
                .eq("voucher_id", message.getVoucherId())
                .one();
        if (sameBusiness != null) {
            if (sameBusiness.getId().equals(message.getOrderId())) {
                return;
            }
            // 正常链路不应为同一业务键生成不同orderId，不能把当前orderId伪装成已创建。
            throw new IllegalStateException("同一用户和优惠券已绑定其他orderId");
        }

        VoucherOrder order = new VoucherOrder();
        order.setId(message.getOrderId());
        order.setUserId(message.getUserId());
        order.setVoucherId(message.getVoucherId());

        // 先插入订单。并发重复消息会被主键/联合唯一索引拦截。
        // 如果后续库存更新失败，MySQL事务会回滚本次INSERT。
        boolean inserted = voucherOrderService.save(order);
        if (!inserted) {
            throw new IllegalStateException("订单保存失败");
        }

        boolean deducted = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", message.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!deducted) {
            // Redis已预扣但MySQL库存不足属于一致性异常，抛出后回滚INSERT并触发重试/DLQ。
            throw new IllegalStateException("数据库库存不足或库存不一致");
        }
    }

    private void verifySameOrder(VoucherOrder existing, SeckillOrderMessage message) {
        if (!existing.getUserId().equals(message.getUserId())
                || !existing.getVoucherId().equals(message.getVoucherId())) {
            throw new IllegalStateException("相同orderId对应不同业务数据");
        }
    }
}
```

并发下仍可能发生：两个消费者都在预查询时看不到订单，其中一个插入成功，另一个遇到唯一键异常。第二个消息会按重试策略再次执行，下一次预查询看到订单后正常返回并 ACK。这是正确的至少一次消费行为。

第一阶段不再依赖 Redisson 用户锁保证正确性。可以暂时保留它减少重复消息竞争，但必须满足：

- 获取锁失败时抛出异常触发重试，不能正常返回；
- 数据库唯一索引仍是最终防线；
- Consumer 与第二阶段补偿任务若都用锁，必须统一锁 Key。

---

## 8. 有限重试和 DLQ

### 8.1 技术环境确认

确认主队列声明中已经包含：

```text
x-dead-letter-exchange = hmdp.seckill.dead.direct
x-dead-letter-routing-key = order.dead
```

确认 DLQ 没有绑定自动消费并直接 ACK 的 Listener。DLQ 是隔离区，默认应由管理台或专用重放工具处理；普通 Listener 自动消费会把诊断证据删掉。

### 8.2 重试行为

配置对应：

```text
第1次处理失败
  → 等待1秒，本地第2次处理
  → 等待2秒，本地第3次处理
  → 仍失败，Reject且不重新进入主队列
  → Broker根据DLX参数路由到DLQ
```

`max-attempts: 3` 包含首次调用，因此总共执行三次，不是“首次 + 三次重试”。

异常分类：

| 场景 | 处理 |
|---|---|
| 已存在相同订单 | 幂等成功，正常返回，ACK |
| MySQL 短暂连接异常 | 抛异常，本地重试 |
| 唯一键并发冲突 | 允许重试；下一次查询应识别已有订单 |
| 消息字段缺失/版本不支持 | 直接失败，最终进入 DLQ |
| MySQL 库存不足 | 一致性异常，重试后进入 DLQ |
| 获取可选 Redisson 锁失败 | 抛异常重试，不得正常返回 |

### 8.3 DLQ 操作原则

第一阶段只实现隔离和人工重放，不实现自动库存补偿。

处理一条 DLQ 消息前：

1. 记录 `messageId/orderId/userId/voucherId/x-death`；
2. 查询 MySQL 是否已有订单；
3. 已有相同订单则属于 ACK 丢失或重复投递，可安全确认；
4. 没有订单则修复根因后，使用相同消息 ID 重投主交换机；
5. 第一阶段不允许看到 DLQ 就直接 `Redis库存+1`，因为可能还有重复消息正在处理。

---

## 9. 切换与回滚顺序

### 9.1 技术环境确认

切换前必须满足：

- Stage 1 逻辑测试全部通过；
- 唯一索引存在；
- RabbitMQ 主队列/DLQ 拓扑正确；
- Stream Pending 为 0，或已记录具体消息；
- 主队列 Ready/Unacked 为 0；
- 有当前版本 Git tag 和数据库备份；
- 只有一个数据库落库链路处于启用状态。

### 9.2 推荐切换顺序

```text
1. 部署 RabbitMQ 拓扑、Publisher、Listener，但 listener-enabled=false
2. 建立唯一索引
3. 启动 RabbitMQ Listener
4. 停止旧 Stream 直接落库消费者
5. 启动 StreamToRabbitRelay
6. 发送10条测试消息
7. 核对订单、库存、Stream Pending、Rabbit Ready/Unacked/DLQ
8. 小流量运行
9. 执行JMeter第一阶段测试
```

### 9.3 回滚

```text
1. 暂停秒杀入口或先停止Relay
2. 等待Rabbit主队列消费完；不要直接清队列
3. 导出并记录DLQ；DLQ消息已被Relay从Stream确认，不能指望旧Stream消费者自动找回
4. 关闭Rabbit Listener
5. 恢复旧Stream数据库消费者
6. 处理Stream Pending
7. 对DLQ逐条核对MySQL；未落库消息保留原orderId重放或人工处置
8. 核对MySQL唯一订单和库存
```

由于 Rabbit 消费可能已经落库，回滚后 Stream 重新消费会遇到重复消息。数据库主键和 `uk_user_voucher` 必须已经存在，确保重复消费只产生幂等结果。DLQ 不允许在回滚时清空；只有订单已落库、已用原消息重放成功，或已有明确人工补偿记录后，才能移除对应死信。

---

## 10. 第一阶段验收标准

必须全部满足：

- RabbitMQ 重复发送同一 `orderId` 10 次，只产生一条数据库订单；
- 同一用户使用不同 `orderId` 购买同一券，只产生一条订单；
- 正常消息事务提交后 AUTO ACK；
- MySQL 暂停后发生三次处理并进入 DLQ，不形成无限热循环；
- 一条毒消息进入 DLQ 时，后续正常消息仍能消费；
- 4 个消费者可以并行处理不同订单；
- RabbitMQ 停止时 Relay 不 XACK Stream；Rabbit 恢复后可以继续转发；
- Relay 在“Rabbit 已收但 Stream 未 XACK”时重复发布，数据库仍只产生一条订单；
- 正常压测最终满足：

```text
超卖数 = 0
重复订单数 = 0
正常场景DLQ数 = 0
Redis成功资格数 = MySQL新增订单数（等待队列排空后）
MySQL剩余库存 = 初始库存 - 新增订单数
```

第一阶段通过后再进入第二阶段，不允许同时修改状态机、补偿、消费者并发和业务表结构后只做一次混合压测，否则无法定位问题来源。

---

## 11. 第一阶段文件级实施清单

| 文件/目录 | 操作 |
|---|---|
| `pom.xml` | 增加 `spring-boot-starter-amqp` |
| `application.yaml` | 增加 Rabbit连接、Confirm、AUTO ACK、并发、重试和功能开关 |
| `src/main/resources/db/hmdp.sql` | 增加 `uk_user_voucher` |
| 现有测试数据库 | 执行联合唯一索引 DDL |
| `seckill.lua` | 第一阶段保留XADD，增加 `createdAt` 字段 |
| `VoucherOrderServiceImpl` | 保留HTTP+Lua入口；停用旧Stream直接落库线程和请求线程初始化proxy逻辑 |
| `com.hmdp.mq.SeckillMqConstants` | 新建MQ常量 |
| `com.hmdp.mq.SeckillOrderMessage` | 新建稳定消息DTO |
| `SeckillRabbitConfiguration` | 新建Exchange、Queue、DLQ、Binding、JSON Converter |
| `ConfirmedSeckillOrderPublisher` | 新建兼容AMQP 2.2的Confirm/Return发布器 |
| `StreamToRabbitRelay` | 新建过渡Relay；Rabbit可靠成功后才XACK Stream |
| `SeckillOrderListener` | 新建Rabbit Listener，AUTO ACK |
| `VoucherOrderTransactionalService` | 新建独立事务Bean，避免AOP自调用问题 |
| `src/test` | 增加DTO、幂等、事务、Confirm、Retry、DLQ测试 |
| `src/test/jmeter` | 后续按第三份计划建立JMX和CSV配置 |

### 参考文档

- [Spring AMQP 2.2 Reference](https://docs.spring.io/spring-amqp/docs/2.2.x/reference/pdf/index.pdf)
- [RabbitMQ Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [Spring Boot 2.3.12 Application Properties](https://docs.spring.io/spring-boot/docs/2.3.12.RELEASE/reference/html/appendix-application-properties.html)
