# 黑马点评 RabbitMQ 改造第二阶段：移除 Stream、状态机与最终一致性闭环

## 1. 阶段目标与最终架构

第二阶段在第一阶段 RabbitMQ 消费链路稳定之后，移除秒杀 Lua 中的 `XADD` 和 Stream Relay，改用 Redis 预占状态与待投递索引保证“Lua 成功但 RabbitMQ 尚未发送”这一空窗期可以恢复。

最终链路：

```text
HTTP请求
  → 生成orderId
  → Lua原子执行
      ├─ 校验Redis库存
      ├─ 校验一人一单
      ├─ Redis库存-1
      ├─ 保存userId → orderId
      ├─ 创建reservation，状态PREPARED
      ├─ ZADD待投递索引
      └─ ZADD活跃预占对账索引
  → 尝试异步投递RabbitMQ
  → 立即返回orderId和PROCESSING

后台投递器/定时扫描器
  → CAS抢占PREPARED/RETRY/过期PUBLISHING
  → 状态PUBLISHING并设置租约
  → RabbitMQ异步Confirm
      ├─ ACK且未Return：PUBLISHED，移除待投递ZSet
      └─ NACK/Return/异常：PUBLISH_RETRY，计算下次重试时间

RabbitMQ Listener
  → AUTO ACK
  → MySQL事务和唯一索引幂等落库
  → Redis技术状态更新为CREATED

永久失败
  → DLQ/投递失败状态
  → 订单级锁
  → 再查MySQL
      ├─ 订单存在：修正为CREATED
      ├─ 仍可恢复：使用原messageId重投
      └─ 明确放弃：Lua原子补偿库存，状态COMPENSATED

Active ZSet
  → 持续索引所有未终态预占
  → 覆盖Rabbit ACK后到CREATED/COMPENSATED之间的对账盲区
```

第二阶段最终达成：

- 秒杀链路完全不依赖 Redis Stream；
- Redis Lua 保证“资格预占 + 可恢复投递意图”原子写入；
- RabbitMQ 至少一次投递；
- 数据库实现业务幂等；
- 投递失败可以自动发现和重投；
- 消费失败进入 DLQ，不阻塞正常消息；
- 订单最终要么创建成功，要么明确补偿；
- 提供订单处理状态查询。

重要边界：这不是 Redis、RabbitMQ、MySQL 的全局 ACID 事务，也不是消息 Exactly Once。系统目标是：

> 局部原子性 + 至少一次投递 + 幂等消费 + 状态机 + 对账补偿 = 最终一致性。

---

## 2. 第二阶段技术环境确认

### 2.1 第一阶段准入条件

必须先满足：

- 第一阶段主队列、DLQ 和消费扩容正常；
- 数据库存在 `uk_user_voucher(user_id, voucher_id)`；
- 重复消息验证通过；
- MySQL 短暂故障可以有限重试；
- RabbitMQ 故障时 Stream Relay 不错误 XACK；
- 第一阶段 JMeter 基线报告已经归档；
- 主队列、DLQ、Stream Pending 都能准确查看；
- 已建立第二阶段独立分支或 Tag。

如果第一阶段仍有重复订单、吞异常 ACK、DLQ 丢消息等问题，不得进入第二阶段。

### 2.2 Redis 服务端确认

查询 Redis 持久化：

```redis
CONFIG GET appendonly
CONFIG GET appendfsync
CONFIG GET save
INFO persistence
INFO replication
```

建议可靠性测试环境至少启用：

```conf
appendonly yes
appendfsync everysec
```

说明：

- `everysec` 仍可能在极端宕机中损失约一秒数据；
- 单节点 Redis 不等于高可用；
- 第二阶段状态机依赖 Redis 持久化，但最终仍需 MySQL 对账；
- 当前方案按单节点 Redis 设计。如果将来改 Redis Cluster，多 Key Lua 必须使用相同 hash slot，不能直接照搬本阶段 Key。

### 2.3 RabbitMQ 确认

第二阶段可靠模式要求：

```yaml
spring.rabbitmq.publisher-confirm-type: correlated
spring.rabbitmq.publisher-returns: true
spring.rabbitmq.template.mandatory: true
```

Confirm 开关可以保留，但关闭时只能作为性能对照模式：

- 不能确认消息是否进入 Broker；
- 不能可靠地从 `PUBLISHING` 迁移到 `PUBLISHED`；
- 会依赖超时重发和数据库幂等，产生更多重复消息；
- 正式可靠性模式必须开启异步 correlated Confirm。

### 2.4 原子性与持久化边界确认

实施人员必须接受以下边界，不能把最终一致性描述成全局事务：

| 范围 | 保证方式 | 持久化位置 |
|---|---|---|
| Redis资格预占 | 单个Lua脚本原子执行 | Redis内存 + AOF/RDB；可靠性取决于服务端配置 |
| Rabbit消息 | durable Exchange/Queue + persistent Message + Confirm | RabbitMQ磁盘；单节点仍有节点故障风险 |
| MySQL订单 | `@Transactional` + InnoDB + 唯一索引 | MySQL数据文件/Redo/Binlog配置 |
| 三者之间 | 状态机、至少一次投递、幂等、对账补偿 | 不存在一个跨三者的ACID事务 |
| JVM回调/线程池 | 不作为可靠记录 | 进程退出即丢失，因此必须由Redis Pending恢复 |

MySQL 是订单最终业务事实来源；Redis reservation 是技术处理记录；RabbitMQ 是异步传输和积压载体。三者职责不能混淆。

---

## 3. Redis 数据模型与状态机

### 3.1 技术环境确认

实施前确认旧 Key 类型：

```redis
TYPE seckill:stock:{voucherId}
TYPE seckill:order:{voucherId}
```

当前 `seckill:order:{voucherId}` 是 Set，不能直接用 `HSET` 改成 Hash，否则会报 `WRONGTYPE`。第二阶段使用新前缀：

```text
seckill:order-map:{voucherId}
```

等第二阶段验证完成后，再决定清理旧 Set；切换期间不要复用同名 Key。

### 3.2 Key 设计

#### Redis 库存

```text
Key: seckill:stock:{voucherId}
Type: String
Value: 剩余库存
```

#### 用户订单映射

```text
Key: seckill:order-map:{voucherId}
Type: Hash
Field: userId
Value: orderId
```

作用：

- 一人一单；
- 用户重复请求时返回原 `orderId`；
- 补偿时确认用户映射仍属于当前订单。

#### 订单预占记录

```text
Key: seckill:reservation:{orderId}
Type: Hash
```

字段：

| 字段 | 作用 |
|---|---|
| `orderId` | 订单ID |
| `messageId` | MQ消息ID，重试保持不变 |
| `userId` | 用户ID |
| `voucherId` | 优惠券ID |
| `status` | 技术状态 |
| `publishAttempts` | 生产投递次数 |
| `leaseUntil` | PUBLISHING租约到期时间 |
| `nextRetryAt` | 下次投递时间 |
| `createdAt` | 预占创建时间 |
| `updatedAt` | 最近更新时间 |
| `lastError` | 最近失败原因摘要 |
| `failureStage` | `PUBLISH`或`CONSUME` |
| `version` | 状态版本，用于审计/CAS |

#### 待投递索引

```text
Key: seckill:publish:pending
Type: ZSet
Member: orderId
Score: 下次需要检查的时间戳
```

Hash 保存状态，ZSet 解决“如何快速找到超时订单”。不能通过 `SCAN seckill:reservation:*` 扫描全部订单做高频调度。

#### 活跃预占对账索引

```text
Key: seckill:reservation:active
Type: ZSet
Member: orderId
Score: 下次对账时间戳
```

`pending` 和 `active` 不能合并：

- `pending` 只回答“哪些订单需要发布/重发到 RabbitMQ”，Broker Confirm 成功后即可移除；
- `active` 回答“哪些预占还没有形成业务终态”，必须一直保留到 `CREATED` 或 `COMPENSATED`；
- `PUBLISHED → 消费失败 → DLQ` 的订单已经不在 `pending`，但仍可通过 `active` 被对账任务发现；
- 对账任务同样不能高频 `SCAN reservation:*`，应分页读取 `active`，每轮限制数量并更新下次检查分数。

如果只建 `pending` 而不建 `active`，Rabbit ACK 后发生的消费失败将缺少可枚举的恢复入口，这是不能上线的恢复盲区。

### 3.3 状态定义

```java
package com.hmdp.seckill;

public enum ReservationStatus {
    /** Redis已经完成资格预占，等待投递。 */
    PREPARED,
    /** 某个实例已经抢到投递权，租约内正在发送。 */
    PUBLISHING,
    /** 上一次投递失败，等待下次重试时间。 */
    PUBLISH_RETRY,
    /** Broker已经ACK且消息没有被Return。 */
    PUBLISHED,
    /** MySQL订单已经提交。 */
    CREATED,
    /** 生产或消费达到终止条件，等待人工决策。 */
    DEAD,
    /** 已获得补偿资格，正在执行反向操作。 */
    COMPENSATING,
    /** Redis库存和用户资格已经恢复。 */
    COMPENSATED
}
```

合法主路径：

```text
PREPARED → PUBLISHING → PUBLISHED → CREATED
              ↓
         PUBLISH_RETRY → PUBLISHING

PUBLISH_RETRY/PUBLISHED → DEAD → COMPENSATING → COMPENSATED
```

禁止：

```text
CREATED → COMPENSATED
COMPENSATED → CREATED
```

MySQL 的订单业务状态（未支付、已支付、已取消）和这里的技术状态是两套概念，不能复用同一个字段。

---

## 4. Lua 原子预占

### 4.1 技术环境确认

确认活动初始化时已经写入：

```redis
GET seckill:stock:{voucherId}
```

确认测试活动不存在旧的 `order-map` 和 reservation 数据。切换测试券时只清理该券和明确订单范围，不允许使用宽泛通配符删除整个 Redis。

当前是单节点 Redis；以下脚本通过多个 `KEYS` 执行。Redis Cluster 下这些 Key 必须落在同一 slot，否则脚本会报 `CROSSSLOT`，需要重新设计 hash tag 和待投递分片。

### 4.2 `seckill_reserve_v2.lua`

约定：

```text
KEYS[1] 库存Key
KEYS[2] 用户订单映射Hash
KEYS[3] reservation Hash
KEYS[4] 待投递ZSet
KEYS[5] 活跃预占对账ZSet

ARGV[1] userId
ARGV[2] orderId
ARGV[3] messageId
ARGV[4] nowMillis
ARGV[5] reservationTtlSeconds
ARGV[6] voucherId
```

脚本：

```lua
local stockKey = KEYS[1]
local orderMapKey = KEYS[2]
local reservationKey = KEYS[3]
local pendingKey = KEYS[4]
local activeKey = KEYS[5]

local userId = ARGV[1]
local orderId = ARGV[2]
local messageId = ARGV[3]
local now = ARGV[4]
local reservationTtl = tonumber(ARGV[5])
local voucherId = ARGV[6]

-- 重复请求返回代码2；Java随后从Hash读取第一次的orderId，不重复扣库存。
-- 不直接return tonumber(orderId)：19位Long转换为Lua number可能发生精度丢失。
local existingOrderId = redis.call('hget', orderMapKey, userId)
if existingOrderId then
    return 2
end

local stock = tonumber(redis.call('get', stockKey))
if (not stock) or stock <= 0 then
    return 1
end

-- 以下操作在同一个Lua中原子完成。
redis.call('decr', stockKey)
redis.call('hset', orderMapKey, userId, orderId)

redis.call('hset', reservationKey,
    'orderId', orderId,
    'messageId', messageId,
    'userId', userId,
    'voucherId', voucherId,
    'status', 'PREPARED',
    'publishAttempts', '0',
    'leaseUntil', '0',
    'nextRetryAt', now,
    'createdAt', now,
    'updatedAt', now,
    'lastError', '',
    'failureStage', '',
    'version', '1'
)

redis.call('expire', reservationKey, reservationTtl)
-- 整个活动的用户映射在最后一次写入后保留相同时间，避免永久占用内存。
redis.call('expire', orderMapKey, reservationTtl)
redis.call('zadd', pendingKey, now, orderId)
redis.call('zadd', activeKey, now, orderId)

-- 0表示本次新建预占成功。
return 0
```

Java 调用：

```java
Long result = stringRedisTemplate.execute(
        RESERVE_SCRIPT,
        Arrays.asList(stockKey, orderMapKey, reservationKey, pendingKey, activeKey),
        userId.toString(),
        orderId.toString(),
        messageId,
        Long.toString(now),
        // TTL必须覆盖活动结束、Rabbit/DLQ最长保留和人工处理窗口。
        Long.toString(TimeUnit.DAYS.toSeconds(30)),
        voucherId.toString()
);

if (result == null) {
    return Result.fail("系统繁忙，请稍后重试");
}
if (result == 1L) {
    return Result.fail("库存不足");
}

Long acceptedOrderId = orderId;
if (result == 2L) {
    Object existing = stringRedisTemplate.opsForHash()
            .get(orderMapKey, userId.toString());
    if (existing == null) {
        // Lua刚返回重复但映射却消失，说明存在过期/并发清理等异常，不能生成新订单。
        return Result.fail("订单状态异常，请稍后查询");
    }
    acceptedOrderId = Long.valueOf(existing.toString());
}

// 新请求使用刚生成的orderId；重复请求返回第一次的orderId。
dispatcher.tryDispatch(acceptedOrderId);
return Result.ok(acceptedOrderId);
```

注意：`dispatcher.tryDispatch()` 即使失败也不能丢掉 `pending ZSet` 和 `active ZSet`。HTTP 返回的语义是“资格已经预占、订单处理中”，不是“MySQL订单已创建”。

---

## 5. CAS 抢占投递权和租约

### 5.1 技术环境确认

确认应用实例时间基本同步。ZSet score 和租约使用应用时间时，多个节点时间漂移会导致提前/延迟重试；生产环境应配置 NTP。开发环境至少确认机器时间一致。

确认同一个 `orderId` 的 `messageId` 永远不变。

### 5.2 抢占 Lua

`claim_publish.lua`：

```lua
local reservationKey = KEYS[1]
local pendingKey = KEYS[2]

local orderId = ARGV[1]
local now = tonumber(ARGV[2])
local leaseMillis = tonumber(ARGV[3])

local status = redis.call('hget', reservationKey, 'status')
if not status then
    redis.call('zrem', pendingKey, orderId)
    return 0
end

local leaseUntil = tonumber(redis.call('hget', reservationKey, 'leaseUntil') or '0')
local nextRetryAt = tonumber(redis.call('hget', reservationKey, 'nextRetryAt') or '0')

local canClaim = status == 'PREPARED'
        or (status == 'PUBLISH_RETRY' and nextRetryAt <= now)
        or (status == 'PUBLISHING' and leaseUntil <= now)

if not canClaim then
    return 0
end

local nextLease = now + leaseMillis
redis.call('hset', reservationKey,
    'status', 'PUBLISHING',
    'leaseUntil', tostring(nextLease),
    'updatedAt', tostring(now),
    'failureStage', ''
)
local publishAttempt = redis.call('hincrby', reservationKey, 'publishAttempts', 1)
redis.call('hincrby', reservationKey, 'version', 1)

-- 应用在发送过程中宕机，租约到期后该订单会再次出现在扫描结果中。
redis.call('zadd', pendingKey, nextLease, orderId)
-- 返回本次物理发送的attempt，失败回调必须携带它做fencing校验。
return publishAttempt
```

租约的意义：

```text
实例A：PREPARED → PUBLISHING，租约5秒
实例B：看到状态正在PUBLISHING，不能重复抢占
实例A宕机：5秒后租约到期
实例B：重新抢占并发送
```

极端情况下仍可能重复发送，但不会静默丢单；数据库幂等负责吸收重复。

---

## 6. 异步 Publisher Confirm 与状态更新

### 6.1 技术环境确认

确认：

- `publisher-confirm-type=correlated`；
- `publisher-returns=true`；
- `mandatory=true`；
- Exchange、Queue、Binding 已声明；
- payload 中的 `messageId` 重试保持不变，`CorrelationData.id` 每次物理发送都生成唯一值；
- 回调中不执行长事务，只更新 Redis 状态和日志。

### 6.2 Dispatcher 骨架

```java
package com.hmdp.seckill;

import com.hmdp.mq.SeckillMqConstants;
import com.hmdp.mq.SeckillOrderMessage;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.UUID;

@Component
public class SeckillMessageDispatcher {
    private final RabbitTemplate rabbitTemplate;
    private final ReservationStateRepository stateRepository;

    public SeckillMessageDispatcher(
            RabbitTemplate rabbitTemplate,
            ReservationStateRepository stateRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.stateRepository = stateRepository;
    }

    public void tryDispatch(Long orderId) {
        long now = System.currentTimeMillis();

        // Lua CAS抢占；返回值是本次物理发送attempt，0/null表示未获得投递权。
        Long publishAttempt = stateRepository.claimForPublish(orderId, now, 5000L);
        if (publishAttempt == null || publishAttempt <= 0L) {
            return;
        }

        SeckillOrderMessage payload = stateRepository.loadMessage(orderId);
        // payload.messageId是稳定业务幂等键；CorrelationData id是每次物理发送的唯一关联键。
        String correlationId = payload.getMessageId()
                + ":" + publishAttempt
                + ":" + UUID.randomUUID();
        CorrelationData correlation = new CorrelationData(correlationId);

        correlation.getFuture().addCallback(
                new ListenableFutureCallback<CorrelationData.Confirm>() {
                    @Override
                    public void onFailure(Throwable ex) {
                        stateRepository.schedulePublishRetry(
                                orderId,
                                publishAttempt,
                                "confirm future failed: " + safeMessage(ex),
                                System.currentTimeMillis()
                        );
                    }

                    @Override
                    public void onSuccess(CorrelationData.Confirm confirm) {
                        Message returned = correlation.getReturnedMessage();

                        if (confirm != null && confirm.isAck() && returned == null) {
                            // Lua脚本只允许合法状态向前推进，不能把CREATED改回PUBLISHED。
                            stateRepository.markPublished(orderId, System.currentTimeMillis());
                            return;
                        }

                        String reason = returned != null
                                ? "message returned"
                                : confirm == null ? "empty confirm" : confirm.getReason();
                        stateRepository.schedulePublishRetry(
                                orderId, publishAttempt, reason, System.currentTimeMillis()
                        );
                    }
                }
        );

        try {
            rabbitTemplate.convertAndSend(
                    SeckillMqConstants.ORDER_EXCHANGE,
                    SeckillMqConstants.ORDER_ROUTING_KEY,
                    payload,
                    message -> {
                        message.getMessageProperties()
                                .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        message.getMessageProperties()
                                .setMessageId(payload.getMessageId());
                        return message;
                    },
                    correlation
            );
        } catch (RuntimeException sendException) {
            // 连接创建等同步异常可能在convertAndSend时直接抛出。
            stateRepository.schedulePublishRetry(
                    orderId,
                    publishAttempt,
                    "send failed: " + safeMessage(sendException),
                    System.currentTimeMillis()
            );
        }
    }

    private String safeMessage(Throwable throwable) {
        String value = throwable == null ? "unknown" : throwable.getMessage();
        if (value == null) {
            return throwable.getClass().getSimpleName();
        }
        return value.length() <= 200 ? value : value.substring(0, 200);
    }
}
```

生产重试建议：

```text
第1次：立即
第2次：1秒
第3次：2秒
第4次：4秒
第5次：8秒
之后：最高30秒间隔
达到最大次数：DEAD，failureStage=PUBLISH
```

不要在 HTTP 请求线程中 `Thread.sleep()` 重试。

### 6.3 状态更新原则

状态更新仍然通过 Lua CAS：

- `markPublished` 只允许 `PUBLISHING/PUBLISH_RETRY/PREPARED → PUBLISHED`；
- 如果已经是 `CREATED`，只清理 Pending，不回退状态；
- 如果已经是 `COMPENSATED`，晚到的 ACK 只能记录告警，不能恢复为 PUBLISHED；
- `schedulePublishRetry` 必须同时比较当前 `publishAttempts` 与回调携带的 attempt；旧尝试的晚到 NACK/Return 不能覆盖新尝试已经得到的 ACK；
- `schedulePublishRetry` 遇到 `PUBLISHED/CREATED/COMPENSATED` 时不再加入 Pending；
- `lastError` 截断长度，避免 Redis 被大异常栈占满。

### 6.4 状态更新 Lua 示例

`update_publish_status`：

```lua
local reservationKey = KEYS[1]
local pendingKey = KEYS[2]
local orderId = ARGV[1]
local now = ARGV[2]

local status = redis.call('hget', reservationKey, 'status')
if not status then
    redis.call('zrem', pendingKey, orderId)
    return 0
end

if status == 'CREATED' then
    -- Consumer可能比Confirm回调更快，不能把CREATED回退成PUBLISHED。
    redis.call('zrem', pendingKey, orderId)
    return 1
end

if status == 'COMPENSATING' or status == 'COMPENSATED' or status == 'DEAD' then
    return 0
end

if status == 'PREPARED' or status == 'PUBLISHING' or status == 'PUBLISH_RETRY' then
    redis.call('hset', reservationKey,
        'status', 'PUBLISHED',
        'updatedAt', now,
        'leaseUntil', '0',
        'nextRetryAt', '0',
        'lastError', '',
        'failureStage', ''
    )
    redis.call('hincrby', reservationKey, 'version', 1)
    redis.call('zrem', pendingKey, orderId)
    return 1
end

return 0
```

`mark_publish_retry.lua` 使用 attempt fencing，避免旧 NACK 覆盖新 ACK：

```lua
local reservationKey = KEYS[1]
local pendingKey = KEYS[2]

local orderId = ARGV[1]
local callbackAttempt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local nextRetryAt = tonumber(ARGV[4])
local lastError = ARGV[5]
local maxAttempts = tonumber(ARGV[6])

local status = redis.call('hget', reservationKey, 'status')
local currentAttempt = tonumber(
        redis.call('hget', reservationKey, 'publishAttempts') or '0')

-- 旧物理发送的晚到失败回调不得修改较新的状态。
if status ~= 'PUBLISHING' or currentAttempt ~= callbackAttempt then
    return 0
end

if currentAttempt >= maxAttempts then
    redis.call('hset', reservationKey,
        'status', 'DEAD',
        'failureStage', 'PUBLISH',
        'lastError', lastError,
        'updatedAt', tostring(now),
        'leaseUntil', '0'
    )
    redis.call('hincrby', reservationKey, 'version', 1)
    redis.call('zrem', pendingKey, orderId)
    return 2
end

redis.call('hset', reservationKey,
    'status', 'PUBLISH_RETRY',
    'failureStage', 'PUBLISH',
    'lastError', lastError,
    'updatedAt', tostring(now),
    'leaseUntil', '0',
    'nextRetryAt', tostring(nextRetryAt)
)
redis.call('hincrby', reservationKey, 'version', 1)
redis.call('zadd', pendingKey, nextRetryAt, orderId)
return 1
```

`mark_created.lua`：

```lua
local reservationKey = KEYS[1]
local pendingKey = KEYS[2]
local activeKey = KEYS[3]
local orderId = ARGV[1]
local now = ARGV[2]

local status = redis.call('hget', reservationKey, 'status')
if not status then
    return 0
end

if status == 'CREATED' then
    redis.call('zrem', pendingKey, orderId)
    redis.call('zrem', activeKey, orderId)
    return 1
end

if status == 'COMPENSATING' or status == 'COMPENSATED' then
    -- 出现这种情况代表补偿和消费发生严重竞态，调用方必须告警。
    return -1
end

redis.call('hset', reservationKey,
    'status', 'CREATED',
    'updatedAt', now,
    'leaseUntil', '0',
    'nextRetryAt', '0',
    'lastError', '',
    'failureStage', ''
)
redis.call('hincrby', reservationKey, 'version', 1)
redis.call('zrem', pendingKey, orderId)
redis.call('zrem', activeKey, orderId)
return 1
```

`ReservationStateRepository` 至少提供这些原子方法：

```java
Long claimForPublish(Long orderId, long now, long leaseMillis);
boolean markPublished(Long orderId, long now);
int schedulePublishRetry(Long orderId, long attempt, String error, long now);
int markCreated(Long orderId, long now);
boolean markCompensating(Long orderId);
boolean compensateAtomically(Reservation reservation);
```

Lua 返回 `-1/0/1/2` 等结果必须在 Java 中完整判断，不能把 `null` 或未知返回码当成功。

---

## 7. 定时投递扫描

### 7.1 技术环境确认

确认 Spring 定时任务已启用：

```java
@EnableScheduling
```

确认扫描器使用批量上限，不能一次取出全部 Pending。确认多实例依靠 `claim_publish.lua` 抢占，而不是依靠单机 `synchronized`。

### 7.2 扫描代码

```java
package com.hmdp.seckill;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class PendingPublishScheduler {
    private final ReservationStateRepository stateRepository;
    private final SeckillMessageDispatcher dispatcher;

    public PendingPublishScheduler(
            ReservationStateRepository stateRepository,
            SeckillMessageDispatcher dispatcher) {
        this.stateRepository = stateRepository;
        this.dispatcher = dispatcher;
    }

    @Scheduled(fixedDelay = 1000L)
    public void dispatchDueReservations() {
        long now = System.currentTimeMillis();
        Set<Long> dueOrderIds = stateRepository.findDueOrderIds(now, 100);

        for (Long orderId : dueOrderIds) {
            // 每条订单由Lua CAS决定哪个实例获得投递权。
            dispatcher.tryDispatch(orderId);
        }
    }
}
```

调度指标至少记录：

- Pending ZSet 数量；
- 本轮扫描数；
- 抢占成功数；
- ACK/NACK/Return 数；
- 平均投递次数；
- 最老 Pending 年龄；
- 达到生产重试上限的订单数。

如果 `pending` 中存在订单但 reservation Hash 不存在，扫描器只能移除该孤儿 member 并发出高优先级告警，不能猜测用户、券或库存并自动补偿。出现该情况通常代表 TTL 配置过短或 Redis 数据异常。

---

## 8. 消费成功后的状态修正

### 8.1 技术环境确认

确认第一阶段 Rabbit Listener、AUTO ACK、唯一索引和事务服务已经通过测试。第二阶段不要同时重写数据库事务逻辑。

确认消费者与补偿任务使用相同订单锁 Key：

```text
lock:seckill:order:{orderId}
```

### 8.2 消费规则

```java
@RabbitListener(queues = SeckillMqConstants.ORDER_QUEUE)
public void onMessage(SeckillOrderMessage message) {
    RLock lock = redissonClient.getLock(
            "lock:seckill:order:" + message.getOrderId()
    );

    boolean acquired = lock.tryLock();
    if (!acquired) {
        // 不能正常返回，否则AUTO会ACK。
        throw new IllegalStateException("订单正在被其他消费者或补偿任务处理");
    }

    try {
        ReservationStatus status = stateRepository.getStatus(message.getOrderId());
        if (status == null) {
            // reservation过期/丢失时无法判断是否已经补偿，不能冒险创建订单。
            throw new IllegalStateException("订单预占记录不存在，需要进入DLQ人工核对");
        }
        if (status == ReservationStatus.COMPENSATED) {
            // 该订单已经明确放弃，晚到的重复消息不能再创建订单。
            return;
        }

        transactionalService.createOrderIdempotently(message);

        try {
            stateRepository.markCreated(message.getOrderId(), System.currentTimeMillis());
        } catch (RuntimeException redisException) {
            // MySQL提交成功就是最终业务事实。Redis状态失败由对账修正，不能重复扣库。
            log.error("订单已落库但Redis状态更新失败, orderId={}",
                    message.getOrderId(), redisException);
        }
    } finally {
        lock.unlock();
    }
}
```

这里有一个重要取舍：

- MySQL 事务成功后，Redis 状态更新失败，不应把业务订单当成失败；
- Listener 可以正常返回并 ACK，后台对账根据 MySQL 修正为 `CREATED`；
- 如果选择抛异常，重复消费也能被数据库幂等吸收，但可能把已经成功的订单送入 DLQ，增加运维噪声；
- 本方案选择“MySQL 是最终业务事实，Redis 状态失败由对账修复”。

---

## 9. 对账与补偿机制

### 9.1 技术环境确认

启用自动补偿前必须确认：

- 数据库唯一索引有效；
- Consumer 和补偿任务使用同一订单锁；
- DLQ 不被普通 Listener 自动 ACK；
- 能查询订单是否存在；
- 能区分“仍在重试”和“明确永久失败”；
- 有补偿操作日志；
- 补偿脚本经过重复执行测试；
- 第一轮只开启“对账报告模式”，暂不自动恢复库存。
- `seckill:reservation:active` 已建立；对账从该索引分页读取，不使用全库 `KEYS`/高频 `SCAN`。

建议上线顺序：

```text
只扫描并打印差异
  → 人工核对一周/完整压测周期
  → 开启自动修正CREATED
  → 最后才开启自动库存补偿
```

### 9.2 对账规则

对账任务分页读取 `active ZSet`。达到业务终态后，从 `active` 移除；未到终态则根据状态把 score 更新为下次检查时间，避免每秒反复扫描同一订单。`active` 自身不设置短 TTL，清理由终态 Lua 完成，并另设“超龄活跃项”告警。

| Redis技术状态 | MySQL订单 | 处理 |
|---|---|---|
| `PREPARED/PUBLISH_RETRY` | 不存在 | 继续投递 |
| `PUBLISHED` | 不存在且未超时 | 等待消费 |
| `PUBLISHED` | 不存在且已进入DLQ/明确终止 | 人工重放或转 `DEAD` |
| 任意非终态 | 存在且数据一致 | 修正为 `CREATED`，清理 Pending |
| `CREATED` | 不存在 | 高危差异，告警，不自动补偿 |
| `DEAD` | 存在 | 修正为 `CREATED` |
| `DEAD` | 不存在 | 可人工重放；明确放弃后补偿 |
| `COMPENSATED` | 存在 | 严重竞态，立即告警 |

禁止根据“超过30秒还没订单”直接恢复库存。必须确认消息已经不再可能被消费，否则会出现库存恢复后晚到消息又创建订单。

### 9.3 补偿 Lua

补偿前，Java 在同一订单锁内完成：

```text
1. 查询MySQL订单不存在
2. 确认状态是DEAD
3. CAS：DEAD → COMPENSATING
4. 调用补偿Lua
```

`compensate_reservation.lua`：

```lua
local stockKey = KEYS[1]
local orderMapKey = KEYS[2]
local reservationKey = KEYS[3]
local pendingKey = KEYS[4]
local activeKey = KEYS[5]

local userId = ARGV[1]
local orderId = ARGV[2]
local now = ARGV[3]

local status = redis.call('hget', reservationKey, 'status')
if status == 'COMPENSATED' then
    -- 重复执行补偿时保持幂等，不重复加库存。
    return 1
end

if status ~= 'COMPENSATING' then
    return 0
end

local mappedOrderId = redis.call('hget', orderMapKey, userId)
if mappedOrderId == orderId then
    redis.call('incr', stockKey)
    redis.call('hdel', orderMapKey, userId)
end

redis.call('hset', reservationKey,
    'status', 'COMPENSATED',
    'updatedAt', now,
    'lastError', '',
    'failureStage', ''
)
redis.call('hincrby', reservationKey, 'version', 1)
redis.call('zrem', pendingKey, orderId)
redis.call('zrem', activeKey, orderId)
return 1
```

为什么检查 `mappedOrderId == orderId`：用户可能已经参与后续活动或映射被新流程修正，旧订单的补偿不能删除别的订单资格。

### 9.4 补偿服务骨架

```java
public void compensateDeadOrder(Long orderId) {
    RLock lock = redissonClient.getLock("lock:seckill:order:" + orderId);
    if (!lock.tryLock()) {
        throw new IllegalStateException("订单正在处理中，稍后重试补偿");
    }

    try {
        Reservation reservation = stateRepository.get(orderId);

        VoucherOrder dbOrder = voucherOrderService.getById(orderId);
        if (dbOrder != null) {
            stateRepository.markCreated(orderId, System.currentTimeMillis());
            return;
        }

        if (reservation.getStatus() != ReservationStatus.DEAD) {
            throw new IllegalStateException("订单尚未进入可补偿终态");
        }

        if (!stateRepository.markCompensating(orderId)) {
            throw new IllegalStateException("补偿状态抢占失败");
        }

        boolean compensated = stateRepository.compensateAtomically(reservation);
        if (!compensated) {
            throw new IllegalStateException("Redis原子补偿失败");
        }
    } finally {
        lock.unlock();
    }
}
```

如果在 `DEAD → COMPENSATING` 后应用宕机，需要定时任务扫描长时间处于 `COMPENSATING` 的记录，在同一订单锁下重新查询 MySQL，再幂等执行补偿。

---

## 10. DLQ 重放与放弃

### 10.1 技术环境确认

确认 DLQ 消息保留，不存在自动消费器。确认具备以下只读信息：

- 消息体；
- `x-death` 次数与时间；
- 原 Exchange/RoutingKey；
- orderId 对应数据库订单；
- Redis reservation 状态。

### 10.2 操作决策

```text
DLQ消息
  → 查询MySQL
      ├─ 已有相同订单：确认该DLQ消息即可，不补库存
      ├─ 数据冲突：告警，禁止自动处理
      └─ 没有订单
          ├─ 根因已恢复：使用原消息ID重投主交换机
          └─ 明确永久放弃：状态DEAD，执行补偿
```

重放必须遵守：

- 使用原 `orderId/messageId`；
- 先 Confirm 主交换机投递成功，再从 DLQ 确认原消息；
- 不允许“先删 DLQ，再尝试重发”；
- 重放操作写审计日志，记录操作人、时间、原因和结果。

第一版可以通过 RabbitMQ 管理台人工重放；后续再开发受权限保护的运维接口。

---

## 11. 订单处理状态查询

### 11.1 技术环境确认

确认接口只能查询当前登录用户自己的订单，不能仅凭 orderId 暴露其他用户数据。Redis reservation TTL 必须长于“活动结束时间 + Rabbit/DLQ最长保留时间 + 人工处理窗口”，示例取30天；清理前还必须确认主队列和DLQ中不存在对应消息。MySQL订单长期保存。

### 11.2 返回模型

```java
@Data
@AllArgsConstructor
public class SeckillOrderStatusDTO {
    private Long orderId;
    /** PROCESSING、SUCCESS、FAILED、COMPENSATED */
    private String status;
    private String message;
}
```

映射：

| 技术状态 | 前端状态 |
|---|---|
| `PREPARED/PUBLISHING/PUBLISH_RETRY/PUBLISHED` | `PROCESSING` |
| `CREATED`或MySQL订单存在 | `SUCCESS` |
| `DEAD` | `FAILED`，提示处理中或联系客服，不立即宣告库存已返还 |
| `COMPENSATED` | `COMPENSATED`，允许重新抢购时再明确提示 |

接口示例：

```java
@GetMapping("/{orderId}/status")
public Result queryStatus(@PathVariable Long orderId) {
    Long currentUserId = UserHolder.getUser().getId();
    return voucherOrderService.querySeckillOrderStatus(orderId, currentUserId);
}
```

查询顺序建议：

```text
先查MySQL订单
  → 存在：SUCCESS，并异步修正Redis CREATED
  → 不存在：查Redis reservation技术状态
```

MySQL 是最终业务事实来源。

---

## 12. 移除 Stream 与切换

### 12.1 技术环境确认

移除前必须满足：

- 状态机单元和集成测试通过；
- RabbitMQ Confirm/Return 故障测试通过；
- Pending ZSet 扫描和租约恢复通过；
- 重复消息幂等通过；
- DLQ 重放通过；
- 补偿重复执行不会重复加库存；
- `COMPENSATED` 晚到消息不会落库；
- 第一阶段 Stream Pending 为 0；
- 主队列和 DLQ 已核对；
- 有回滚版本。

### 12.2 切换顺序

```text
1. 暂停创建新的秒杀请求
2. 等待第一阶段Stream、Rabbit主队列排空
3. 部署第二阶段代码，但保留功能开关
4. 初始化测试券的新order-map前缀
5. 启用reservation、pending调度器和active对账索引
6. 切换Lua到seckill_reserve_v2.lua
7. 关闭Stream Relay
8. 发送10条测试订单并完整核对
9. 恢复小流量
10. 执行第二阶段故障和JMeter测试
```

### 12.3 第二阶段回滚

不能直接关掉 V2 再恢复 Stage 1。V2 已受理的订单没有写入 Stream，粗暴回滚会把这些订单留在 Redis 预占状态中。

```text
1. 暂停新的秒杀请求
2. 关闭自动补偿，停止V2产生新的反向状态变化
3. 停止V2 Publisher调度器，但暂不清理pending/active/reservation
4. 等待Rabbit主队列处理完，并单独记录DLQ消息
5. 对active逐条核对MySQL：已落库→CREATED；未落库→明确重放或人工转DEAD
6. 只有pending=0、active无未决项、主队列=0且DLQ有处理清单后，才切回Stage 1 Lua
7. 启动Stage 1 Stream Relay和Rabbit Listener，先做10条Smoke Test
8. 恢复小流量并继续保留V2 Hash/ZSet，观察一个稳定周期
```

如果无法在维护窗口内清零 V2 未决项，应继续保留 V2 Dispatcher/Reconciler 只处理存量，同时 Stage 1 处理新流量；两套链路必须使用明确的券批次或版本字段隔离，不能让同一笔订单被两个补偿器处理。

### 12.4 暂缓删除旧代码和旧数据

至少保留一个稳定观察周期后再清理：

- 旧 Stream Relay 代码；
- 旧 `seckill:order:{voucherId}` Set；
- `stream.order`；
- 第一阶段开关。

清理前先导出/记录 Stream Pending 和订单对账结果。不要在切换当天同时删除回滚能力。

---

## 13. 第二阶段验收标准

功能与幂等：

- 同一用户重复请求返回相同 orderId；
- 同一消息重复投递 10 次只创建一条订单；
- 不同 orderId 但相同 userId/voucherId 只创建一条订单；
- 补偿脚本重复执行 10 次只恢复一次库存；
- `CREATED` 不允许迁移到 `COMPENSATED`；
- `COMPENSATED` 的晚到消息不会创建数据库订单。

投递可靠性：

- Lua 成功后、发送前杀进程，重启后 Pending ZSet 能补发；
- Rabbit 已收但 Confirm 回调前杀进程，允许重复投递但不重复落库；
- RoutingKey 错误触发 Return，状态不进入 PUBLISHED；
- RabbitMQ 停机期间订单保留在 PREPARED/PUBLISH_RETRY；
- Rabbit 恢复后自动发送并最终 CREATED。

消费可靠性：

- MySQL 停机时有限重试并进入 DLQ；
- MySQL 恢复后使用原消息重放成功；
- 事务提交后、ACK 前杀消费者，重新投递后幂等成功；
- 毒消息进入 DLQ，不阻塞正常消息。

一致性：

```text
正常场景：
  Redis成功预占数 = MySQL新增订单数
  Redis剩余库存 = 初始库存 - 成功预占数
  MySQL剩余库存 = 初始库存 - MySQL新增订单数
  Pending数 = 0
  Active数 = 0
  DLQ数 = 0

明确补偿后：
  MySQL订单不存在
  Redis库存恢复1次
  userId → orderId映射删除
  status = COMPENSATED
  Pending中不存在该orderId
  Active中不存在该orderId
```

性能验收不预设虚假“提升百分比”，以第三份测试计划中的相同环境 A/B 结果为准。第二阶段入口增加 Redis Hash/ZSet 写入和 RabbitMQ 发布，HTTP 延迟可能比纯 Stream 略高；主要收益是消费扩容、失败隔离、可观测恢复和最终一致性，而不是承诺入口 TPS 必然提高。

---

## 14. 第二阶段文件级实施清单

| 文件/目录 | 操作 |
|---|---|
| `seckill_reserve_v2.lua` | 新建无XADD的原子预占脚本 |
| `claim_publish.lua` | 新建投递抢占、租约和attempt脚本 |
| `update_publish_status` | 新建ACK状态迁移脚本 |
| `mark_publish_retry.lua` | 新建带attempt fencing的重试/DEAD脚本 |
| `mark_created.lua` | 新建落库成功状态脚本 |
| `mark_compensating.lua` | 新建 `DEAD → COMPENSATING` CAS脚本 |
| `compensate_reservation.lua` | 新建幂等库存和资格补偿脚本 |
| `ReservationStatus` | 新建技术状态枚举 |
| `SeckillReservation` | 新建Redis预占模型，不与MySQL业务状态混用 |
| `ReservationStateRepository` | 封装Hash/ZSet/Lua，不在Service散写状态 |
| `SeckillMessageDispatcher` | 新建异步Confirm发布器 |
| `PendingPublishScheduler` | 新建批量到期扫描与重投 |
| `ReservationReconciliationService` | 分页扫描active ZSet做MySQL/Redis对账，先report-only |
| `ReservationCompensationService` | 新建订单锁、DB复查、原子补偿 |
| `VoucherOrderServiceImpl` | 切换到V2预占脚本；去掉Stream和Relay依赖 |
| `SeckillOrderListener` | 增加统一订单锁、COMPENSATED/缺失状态保护、CREATED修正 |
| `VoucherOrderController` | 增加用户归属校验后的订单状态查询 |
| `application.yaml` | 增加状态机、扫描批量、租约、重试上限、report-only开关 |
| `src/test` | 增加状态竞态、宕机窗口、对账和补偿幂等测试 |

### 建议配置开关

```yaml
hmdp:
  seckill:
    reservation-enabled: true
    pending-dispatch-enabled: true
    reconciliation-mode: report-only # report-only / repair-status / full
    auto-compensation-enabled: false  # 最后一个开启的开关
    publish-lease-millis: 5000
    publish-scan-batch-size: 100
    max-publish-attempts: 10
    reservation-retention-days: 30
```

### 参考文档

- [Redis Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [RabbitMQ Publisher Confirms](https://www.rabbitmq.com/tutorials/tutorial-seven-java)
- [Spring AMQP CorrelationData 2.2.18 API](https://docs.spring.io/spring-amqp/docs/2.2.18.RELEASE/api/org/springframework/amqp/rabbit/connection/CorrelationData.html)
