# 黑马点评 RabbitMQ 两阶段改造：完整逻辑、故障与 JMeter 性能测试计划

## 1. 测试目标

本计划用于验证三个版本：

```text
V6：当前 Redis Stream 直接异步落库
V7.1：Stream → Rabbit Relay → Rabbit多消费者落库
V7.2：Redis状态机/Pending+Active ZSet → Rabbit → Rabbit多消费者落库
```

测试不能只看 HTTP TPS。秒杀接口是异步接口，HTTP 返回成功只表示“资格已受理”，还必须验证：

- 最终数据库订单数；
- Redis 与 MySQL 库存；
- 一人一单；
- Rabbit Ready/Unacked/DLQ；
- Stream Pending（V6/V7.1）；
- Redis Pending/Active ZSet 和 reservation 状态（V7.2）；
- 从请求受理到 MySQL 订单提交的端到端延迟；
- 中间件故障后的恢复时间；
- 是否存在消息丢失、重复订单或错误补偿。

核心验收原则：

```text
消息允许重复投递
业务不允许重复生效

系统允许短暂不一致
最终必须创建订单或明确补偿
```

---

## 2. 测试技术环境总确认

### 2.1 环境隔离

压力和故障测试必须使用独立测试环境：

| 组件 | 测试隔离建议 |
|---|---|
| MySQL | 独立数据库 `hmdp_test` |
| Redis | 独立实例或独立 DB，例如 database 15 |
| RabbitMQ | 独立 vhost `/hmdp-test` |
| 应用 | `application-loadtest.yaml`，禁止连接真实数据 |
| JMeter | 单独压测机，避免与应用争用 CPU |

只有确认当前连接的是测试环境，才允许执行清库、`FLUSHDB` 或 Queue Purge。禁止对共享 Redis DB、默认 Rabbit vhost 或真实 MySQL 执行破坏性测试。

### 2.2 版本与资源记录

每份报告记录：

```text
Git commit/版本：
Java版本：
JVM参数：
Spring Boot版本：
Redis版本与持久化配置：
RabbitMQ版本、节点数和队列类型：
MySQL版本与连接池配置：
应用实例数：
Consumer concurrency/max-concurrency/prefetch：
Publisher Confirm开关：
服务器CPU/内存/磁盘：
JMeter版本和压测机配置：
网络位置和延迟：
日志级别：INFO/WARN，禁止DEBUG压测：
```

### 2.3 基础命令

```powershell
java -version
mvn -version
jmeter --version

Test-NetConnection -ComputerName <app-host> -Port 8081
Test-NetConnection -ComputerName <rabbit-host> -Port 5672
Test-NetConnection -ComputerName <redis-host> -Port 6379
Test-NetConnection -ComputerName <mysql-host> -Port 3306
```

### 2.4 测试工具

- JUnit 5：单元测试；
- Mockito：纯 Java 分支测试；
- `@SpringBootTest`：集成测试；
- Redis/RabbitMQ/MySQL 真实测试实例：Lua、ACK、DLQ、事务测试；
- JMeter 5.6.x：接口和并发压力；
- RabbitMQ Management：Ready、Unacked、Redelivered、DLQ；
- Redis CLI：库存、映射、reservation、Pending、Active；
- MySQL：订单数、重复数据、库存和锁等待；
- JVM/系统监控：CPU、内存、GC、线程、网络、磁盘。

---

## 3. 测试数据与安全重置

### 3.1 技术环境确认

重置前打印并人工确认：

```text
当前Spring Profile=loadtest
MySQL schema=hmdp_test
Redis database=15或独立测试实例
Rabbit vhost=/hmdp-test
测试voucherId
初始库存
预计用户数
```

如果任意一项不匹配，停止测试。

### 3.2 用户和 Token

当前项目的 `generateJmeterUserTokens()` 固定生成 1000 个 Token，适合 1000 用户以内场景。建议生成 CSV：

```text
token,userId
load-test-1,1
load-test-2,2
...
```

大规模测试需要专用数据准备程序，支持参数：

```text
userCount=1000/10000/60000
tokenTtlMinutes=120
output=target/jmeter/user-tokens.csv
```

要求：

- 每个成功资格请求使用不同用户；
- CSV 不循环使用 Token；
- 数据准备时间不计入压测；
- Token TTL 覆盖整个压测周期；
- 同用户重复测试使用单独 CSV，只放一个 Token。

### 3.3 每轮测试重置顺序

在隔离测试环境执行：

```text
1. 停止压测流量
2. 等待或暂停消费者
3. 记录Rabbit/Stream/Pending/DLQ残留
4. 清理测试券订单
5. 重置MySQL库存
6. 清理该测试券Redis数据
7. 清空测试vhost中的测试队列
8. 重新预热Redis库存
9. 启动应用和消费者
10. 等待30秒稳定
11. 执行10请求Smoke Test
12. 执行正式测试
```

MySQL 示例：

```sql
SET @voucher_id = 10001;
SET @initial_stock = 1000;

DELETE FROM tb_voucher_order WHERE voucher_id = @voucher_id;

UPDATE tb_seckill_voucher
SET stock = @initial_stock
WHERE voucher_id = @voucher_id;

SELECT voucher_id, stock
FROM tb_seckill_voucher
WHERE voucher_id = @voucher_id;
```

Redis V6/V7.1：

```redis
DEL seckill:stock:10001
DEL seckill:order:10001
SET seckill:stock:10001 1000
```

Redis V7.2：

```redis
DEL seckill:stock:10001
DEL seckill:order-map:10001
SET seckill:stock:10001 1000
```

reservation、全局 Pending 和 Active 的清理优先使用独立 Redis 测试 DB，在确认 DB 编号后执行 `FLUSHDB`。如果无法隔离 Redis DB，必须维护本轮 orderId 清单，逐条删除 reservation、Pending member 和 Active member，禁止宽泛执行 `KEYS ... | DEL`。

第二阶段测试结束时 `pending=0` 只能证明没有待发布消息，不能证明所有 Rabbit 已发布订单都已落库；必须同时检查 `seckill:reservation:active=0`，或保证每个残留项都有明确的 DLQ/人工处理结论。

RabbitMQ 只清空 `/hmdp-test` 中：

```text
hmdp.seckill.order.queue
hmdp.seckill.order.dlq
```

不要删除 Exchange/Queue 定义，除非当前测试就是拓扑重建测试。

---

## 4. 编译、静态和单元测试

### 4.1 技术环境确认

确认 Maven 使用当前项目 JDK 和仓库配置。确认单元测试不连接真实 Redis/MySQL/RabbitMQ；需要真实中间件的测试放到 `integration` Profile。

### 4.2 编译门禁

```powershell
mvn -DskipTests clean package
```

预期：

- 编译成功；
- 不出现 Spring AMQP 新版本 API 找不到；
- 不出现 `ReturnsCallback`/`CompletableFuture` 版本错误；
- 不出现循环依赖；
- 不出现多个 `MessageConverter` 冲突。

### 4.3 单元测试清单

#### Stage 1

| 编号 | 测试 | 预期 |
|---|---|---|
| U1-01 | Rabbit消息DTO字段完整 | 校验通过 |
| U1-02 | 缺少orderId/userId/voucherId | 抛异常，不能正常ACK |
| U1-03 | 相同orderId已存在且业务字段一致 | 幂等返回，不更新库存 |
| U1-04 | 相同orderId但userId不同 | 抛数据冲突异常 |
| U1-05 | 同userId/voucherId、相同orderId已有订单 | 幂等返回；不同orderId视为数据冲突 |
| U1-06 | MySQL库存更新返回false | 事务抛异常并回滚订单INSERT |
| U1-07 | Publisher ACK且未Return | Relay允许XACK Stream |
| U1-08 | Publisher NACK | Relay不XACK Stream |
| U1-09 | Publisher ACK但ReturnedMessage非空 | Relay不XACK Stream |
| U1-10 | Confirm超时 | Relay不XACK并退避 |

#### Stage 2

| 编号 | 测试 | 预期 |
|---|---|---|
| U2-01 | 首次预占 | 库存-1、映射/Hash/ZSet同时写入 |
| U2-02 | 同用户重复预占 | 返回原orderId，库存不再减少 |
| U2-03 | 库存为0 | 返回-1，不创建reservation |
| U2-04 | 两实例抢占相同PREPARED | 只有一个CAS成功 |
| U2-05 | PUBLISHING租约未到期 | 不能抢占 |
| U2-06 | PUBLISHING租约到期 | 可以重新抢占 |
| U2-07 | CREATED收到晚到Confirm | 状态不回退到PUBLISHED |
| U2-08 | COMPENSATED收到晚到Confirm | 状态不变化并告警 |
| U2-09 | 补偿Lua执行10次 | 库存只恢复一次 |
| U2-10 | userId映射已指向其他orderId | 旧补偿不能删除新映射/增加库存 |
| U2-11 | 旧attempt的NACK晚于新attempt的ACK | 状态保持PUBLISHED，不回退到RETRY |

单元测试通过标准：全部通过，无随机失败；状态迁移测试重复运行 100 次结果一致。

---

## 5. 数据库与幂等集成测试

### 5.1 技术环境确认

确认连接 `hmdp_test`，并执行：

```sql
SHOW INDEX FROM tb_voucher_order;
```

必须存在：

```text
PRIMARY(id)
uk_user_voucher(user_id, voucher_id)
```

### 5.2 测试用例

| 编号 | 操作 | 预期结果 |
|---|---|---|
| DB-01 | 相同orderId插入两次 | 第二次主键冲突，最终1条 |
| DB-02 | 不同orderId、相同userId/voucherId | 第二次联合唯一键冲突，最终1条 |
| DB-03 | 相同userId、不同voucherId | 可以各创建1条 |
| DB-04 | 不同userId、相同voucherId | 可以各创建1条，受库存限制 |
| DB-05 | INSERT成功后库存更新失败 | 整个事务回滚，订单不存在 |
| DB-06 | 100线程重复消费同一消息 | 最终1条订单、MySQL库存只减1 |
| DB-07 | 100线程不同用户、库存100 | 最终100条订单、库存为0 |

查询重复订单：

```sql
SELECT user_id, voucher_id, COUNT(*) AS c
FROM tb_voucher_order
GROUP BY user_id, voucher_id
HAVING COUNT(*) > 1;
```

预期永远为空。

---

## 6. RabbitMQ ACK、重试和 DLQ 逻辑测试

### 6.1 技术环境确认

确认配置：

```yaml
acknowledge-mode: auto
default-requeue-rejected: false
retry.enabled: true
retry.max-attempts: 3
```

确认主队列绑定 DLX，DLQ 没有自动 Listener。

### 6.2 用例

| 编号 | 注入故障 | 预期 |
|---|---|---|
| MQ-01 | 正常消息 | DB提交后ACK，Ready/Unacked最终为0 |
| MQ-02 | Listener抛RuntimeException | 总共调用3次，然后进入DLQ |
| MQ-03 | 非法JSON/字段缺失 | 不产生订单，最终进入DLQ |
| MQ-04 | 一条毒消息后发送10条正常消息 | 毒消息隔离，正常消息全部落库 |
| MQ-05 | DB事务耗时期间查看管理台 | 消息显示Unacked |
| MQ-06 | DB提交前杀消费者 | 消息重新投递，最终创建1条订单 |
| MQ-07 | DB提交后、ACK前杀消费者 | 重新投递但幂等，库存只减1 |
| MQ-08 | 手工重复发布同一消息10次 | 只有1条订单 |

预期重试时间近似：

```text
t=0 第1次
t≈1s 第2次
t≈3s 第3次
随后进入DLQ
```

允许少量调度误差，不允许毫秒级无限重入队。

---

## 7. Stage 1 故障恢复测试

### 7.1 技术环境确认

确认运行的是：

```text
Lua XADD开启
旧Stream数据库消费者关闭
StreamToRabbitRelay开启
Rabbit Listener开启
```

确认开始前 Stream Pending、Rabbit Ready/Unacked/DLQ 均为0。

### 7.2 用例

| 编号 | 故障时机 | 预期结果 |
|---|---|---|
| S1-F01 | RabbitMQ停机后发起100个秒杀 | HTTP资格进入Stream；Relay不XACK；Rabbit恢复后全部转发 |
| S1-F02 | Rabbit已接收、Relay执行XACK前杀进程 | Stream消息重新处理；Rabbit可能重复；DB仍只1条/用户 |
| S1-F03 | RoutingKey故意配置错误 | 触发Return；Stream不XACK |
| S1-F04 | Exchange不存在 | NACK/异常；Stream不XACK |
| S1-F05 | MySQL停机 | Rabbit消费重试后DLQ；Stream已完成Relay但DLQ保留消息 |
| S1-F06 | 恢复MySQL并重放DLQ | 使用原orderId后创建成功 |

验收：任何测试都不得出现“Stream已ACK、Rabbit无消息、MySQL无订单”的静默丢失状态。

---

## 8. Stage 2 状态机、投递补偿和对账测试

### 8.1 技术环境确认

确认运行的是：

```text
Lua不再XADD
Stream Relay关闭
reservation/Pending ZSet开启
Publisher Confirm开启
Pending Scheduler开启
Rabbit Listener开启
自动库存补偿初始为report-only
```

确认 Redis AOF 配置和测试 DB 隔离。

### 8.2 用例

| 编号 | 故障时机 | 预期状态/结果 |
|---|---|---|
| S2-F01 | Lua预占后、调用Publisher前杀应用 | PREPARED留在ZSet；重启后自动发送并CREATED |
| S2-F02 | 状态PUBLISHING后、发送前杀应用 | 租约到期后重新抢占并发送 |
| S2-F03 | Rabbit已收、Confirm回调前杀应用 | 可能重复发送；DB只1条；最终CREATED |
| S2-F04 | Confirm ACK后、markPublished前杀应用 | Pending重发；DB幂等；最终CREATED |
| S2-F05 | Rabbit停机10分钟 | reservation保持PREPARED/RETRY，不丢失；恢复后补发 |
| S2-F06 | 错误RoutingKey | ReturnedMessage存在；不能进入PUBLISHED |
| S2-F07 | MySQL提交后Redis状态更新失败 | MySQL订单存在；对账修正CREATED；不补库存 |
| S2-F08 | 消费重试耗尽 | 消息进入DLQ；状态等待人工决策，不自动恢复库存 |
| S2-F08A | Rabbit已Confirm、消费失败进入DLQ | 订单已离开pending但仍在active，可被对账任务发现 |
| S2-F09 | DEAD订单MySQL不存在，执行补偿 | Redis库存+1一次、映射删除、COMPENSATED |
| S2-F10 | 对同一订单并发执行10次补偿 | 仅一个CAS成功，库存只+1 |
| S2-F11 | 补偿完成后投递一条旧消息 | Consumer看到COMPENSATED，不创建订单 |
| S2-F12 | Consumer与补偿器同时处理 | 同一订单锁串行；不出现订单存在且库存已恢复 |

### 8.3 高危不变量

每轮故障测试后查询：

```text
不存在 CREATED → COMPENSATED 回退
不存在 COMPENSATED 且 MySQL已有订单
不存在 Redis库存恢复两次
不存在 userId映射被旧订单补偿误删
不存在 Pending永久增长且无告警
```

自动补偿只有在以上测试全部通过后才能从 `report-only` 切换为 `enabled`。

---

## 9. JMeter 测试计划结构

### 9.1 技术环境确认

确认使用非 GUI 模式正式压测。GUI 只用于编辑和 Smoke Test。

JMeter 压测机：

- 不与应用、Redis、RabbitMQ、MySQL 同机；
- CPU 长时间低于 80%；
- 无大量 GC；
- 网络带宽不是瓶颈；
- `user-tokens.csv` 行数不少于总请求数；
- CSV `Recycle on EOF=false`、`Stop thread on EOF=true`、`Sharing mode=All threads`。

### 9.2 JMX 参数

JMX 中统一使用属性：

| 属性 | 默认值 | 说明 |
|---|---:|---|
| `protocol` | `http` | 协议 |
| `host` | `127.0.0.1` | 应用地址 |
| `port` | `8081` | 应用端口 |
| `voucherId` | 无 | 必填测试券ID |
| `threads` | `100` | 并发线程数 |
| `rampUp` | `10` | 启动时间，秒 |
| `loops` | `1` | 每线程循环次数 |
| `duration` | `0` | 持续时间，0表示按loops |
| `csvFile` | `target/jmeter/user-tokens.csv` | Token数据 |
| `connectTimeout` | `2000` | 连接超时ms |
| `responseTimeout` | `3000` | 响应超时ms |
| `targetRps` | `0` | Constant Throughput Timer目标，0为不限速 |

JMeter 表达式：

```text
${__P(host,127.0.0.1)}
${__P(port,8081)}
${__P(voucherId)}
${__P(threads,100)}
${__P(rampUp,10)}
${__P(loops,1)}
${__P(csvFile,target/jmeter/user-tokens.csv)}
```

### 9.3 JMX 组件

```text
Test Plan
  ├─ User Defined Variables
  ├─ HTTP Request Defaults
  ├─ HTTP Header Manager
  │    ├─ authorization: ${token}
  │    └─ Content-Type: application/json
  ├─ CSV Data Set Config
  │    ├─ Variable Names: token,userId
  │    ├─ Recycle on EOF: false
  │    ├─ Stop thread on EOF: true
  │    └─ Sharing mode: All threads
  ├─ Thread Group
  │    ├─ Synchronizing Timer（仅瞬时洪峰场景）
  │    ├─ HTTP POST /voucher-order/seckill/${voucherId}
  │    ├─ JSON/Javascript处理业务结果
  │    └─ Assertions
  └─ Simple Data Writer（写JTL，不使用GUI监听器）
```

### 9.4 业务结果分类

接口业务失败可能仍然返回 HTTP 200，不能只看 HTTP 错误率。使用 JSR223 PostProcessor 给 Sample 重命名：

```groovy
import groovy.json.JsonSlurper

def body = new JsonSlurper().parseText(prev.getResponseDataAsString())
if (body.success == true) {
    prev.setSampleLabel('seckill-accepted')
} else {
    def msg = String.valueOf(body.errorMsg ?: body.message ?: '')
    if (msg.contains('库存不足')) {
        prev.setSampleLabel('seckill-sold-out')
    } else if (msg.contains('重复') || msg.contains('不能重复')) {
        prev.setSampleLabel('seckill-duplicate')
    } else {
        prev.setSampleLabel('seckill-system-failure')
    }
}
```

同时增加断言：

- HTTP 状态必须为 200；
- 响应必须是合法 JSON；
- `success=true` 时必须有 orderId；
- 不允许空响应；
- `seckill-system-failure` 单独统计，不能混入库存不足。

---

## 10. JMeter 场景和参数

### 10.1 Smoke Test

技术环境确认：库存10、10个不同用户、队列为空、无历史订单。

```text
threads=10
rampUp=2
loops=1
initialStock=10
userCount=10
```

预期：

- 10 个 `seckill-accepted`；
- 最终 MySQL 10 条订单；
- Redis/MySQL 库存均为0；
- DLQ=0；
- Pending最终=0。

### 10.2 全部成功容量测试

技术环境确认：准备10000个唯一Token，库存10000；CSV不循环。

```text
threads=200
rampUp=30
loops=50
totalRequests=10000
initialStock=10000
```

同一线程每次循环会从共享 CSV 读取下一行，因此每个请求必须获得新的 Token。

预期：

- accepted=10000；
- 最终订单=10000；
- 重复订单=0；
- 超卖=0；
- 正常DLQ=0；
- 队列最终排空。

### 10.3 瞬时秒杀洪峰

技术环境确认：1000个唯一用户、库存100、Synchronizing Timer准备释放1000线程。

```text
threads=1000
rampUp=1
loops=1
Synchronizing Timer groupSize=1000
timeout=10000ms
initialStock=100
```

预期：

- accepted=100；
- sold-out≈900；
- 最终订单=100；
- Redis/MySQL库存=0；
- 超卖=0；
- 重复订单=0。

如果部分线程因客户端资源不足无法同步启动，需要降低线程或使用多台 JMeter，不能把压测机瓶颈当成应用结果。

### 10.4 同用户重复请求

技术环境确认：该 Thread Group 禁用共享 CSV，改用 User Defined Variable/固定 Header 使用同一个 Token；或者准备500行内容相同的专用 CSV。只放一行且 `Recycle=false` 会导致其他线程直接停止，不能用于本场景。库存100。

```text
threads=500
rampUp=1
loops=1
initialStock=100
```

预期：

- V6/V7.1：1个成功，其余提示重复；
- V7.2：所有重复请求应返回相同orderId，或按最终接口约定统计为同一资格；
- MySQL订单=1；
- Redis/MySQL库存只减少1；
- `uk_user_voucher` 无重复。

### 10.5 Consumer 扩容和积压清空

技术环境确认：停止 Rabbit Listener，但保持入口/Publisher可用；准备10000唯一用户和库存。

```text
生产积压：threads=200, loops=50, total=10000
分别测试消费配置：
  A: concurrency=1, prefetch=10
  B: concurrency=4, prefetch=10
  C: concurrency=8, prefetch=10
```

记录：

```text
开始Ready数量
开始消费时间
Ready归零时间
平均消费TPS
MySQL CPU/锁等待/连接池
```

预期：

- 4消费者明显快于1消费者；
- 8消费者是否继续提升由MySQL决定；
- 如果8消费者锁等待和错误显著增加，则最终配置选择4而不是盲目追求并发；
- 所有配置最终订单数一致且无重复。

### 10.6 Prefetch 对比

技术环境确认：consumer concurrency固定为4，其他环境相同。

```text
prefetch=1
prefetch=10
prefetch=50
```

预期：

- `1` 最保守，吞吐通常最低；
- `10` 作为初始推荐；
- `50` 可能提高吞吐，也可能增加Unacked和消费者内存；
- 最终选择基于吞吐、P99、Unacked和故障重投量共同决定。

### 10.7 Publisher Confirm 开关对比

技术环境确认：只在 V7.2 性能对照环境执行；关闭 Confirm 时明确标注“可靠性降级”，不执行为正式可靠性结论。

```text
A: correlated Confirm=true
B: Confirm=none
相同10000请求、相同消费者、相同数据
```

记录 HTTP TPS/P95/P99、Publisher CPU、Rabbit磁盘和重复投递。

预期：关闭 Confirm 的 Publisher 吞吐可能更高，但无法确认 Broker 接收结果。最终生产建议不能只因吞吐差异关闭核心订单 Confirm。

### 10.8 30分钟稳定性测试

技术环境确认：准备至少60000个唯一Token，目标30请求/秒，库存60000；确保Token TTL大于测试时间。

```text
threads=100
rampUp=60
duration=1800s
Loop Forever=true
Constant Throughput Timer=1800 samples/minute
```

预期：

- TPS围绕30/s；
- HTTP系统错误率<0.1%，理想为0；
- 正常场景DLQ=0；
- Pending/Ready不持续单调增长；
- Active不持续单调增长，队列排空并对账后归零；
- JVM无持续内存泄漏；
- GC、MySQL连接池和Rabbit Unacked稳定；
- 最终成功资格与订单数一致。

### 10.9 端到端异步延迟

V7.2 从秒杀响应提取 `orderId`，单独运行状态轮询脚本：

```text
POST 秒杀
  → 提取orderId和请求时间
  → 每100ms GET /voucher-order/{orderId}/status
  → 最多轮询50次
  → SUCCESS时记录端到端耗时
```

状态轮询会增加系统负载，因此与纯入口TPS测试分开运行。

预期初始目标：

- 正常无积压时绝大多数订单数秒内进入 SUCCESS；
- 具体 P95/P99 以本机基线确定；
- HTTP入口很快但订单长时间不落库，不能算性能提升。

---

## 11. 非 GUI 执行命令

```powershell
$resultRoot = 'D:\SeekJob\hm-dianping\hm-dianping\target\jmeter-results\stage2-run01'
New-Item -ItemType Directory -Path $resultRoot -Force | Out-Null

jmeter -n `
  -t 'D:\SeekJob\hm-dianping\hm-dianping\src\test\jmeter\seckill.jmx' `
  -Jhost=127.0.0.1 `
  -Jport=8081 `
  -JvoucherId=10001 `
  -Jthreads=200 `
  -JrampUp=30 `
  -Jloops=50 `
  -JcsvFile='D:\SeekJob\hm-dianping\hm-dianping\target\jmeter\user-tokens.csv' `
  -l "$resultRoot\result.jtl" `
  -e `
  -o "$resultRoot\html"
```

每次 HTML 报告输出目录必须为空或使用新的 run 编号，避免 JMeter 拒绝生成报告或混入旧结果。

不要在正式压测中启用 View Results Tree、Graph Results 等 GUI Listener；它们会消耗大量客户端内存。

---

## 12. 指标、计算公式与结果模板

### 12.1 必采指标

#### HTTP

- 请求数；
- accepted/sold-out/duplicate/system-failure；
- TPS；
- P50/P90/P95/P99；
- 最大响应时间；
- 网络/HTTP错误率。

#### RabbitMQ

- Publish rate、Confirm ACK/NACK、Return；
- Deliver/Ack rate；
- Ready、Unacked；
- Redelivered；
- DLQ数量；
- 最老消息年龄；
- Rabbit CPU、内存、磁盘。

#### Redis

- Lua调用耗时；
- Redis库存；
- 用户映射数量；
- Pending ZSet数量；
- Active ZSet数量与最老Active年龄；
- PREPARED/PUBLISHING/RETRY/CREATED/DEAD/COMPENSATED数量；
- Redis CPU、内存、命令延迟和持久化状态。

#### MySQL

- 订单新增数；
- 剩余库存；
- 重复订单查询结果；
- TPS；
- 活跃连接/等待连接；
- 行锁等待和死锁；
- CPU、IO。

#### JVM

- CPU；
- Heap/Non-Heap；
- GC次数与暂停；
- Rabbit Listener线程；
- 数据库连接池；
- 异常日志数量。

### 12.2 正确性公式

设：

```text
S0 = 初始库存
A  = Redis接受资格数
O  = 最终MySQL新增订单数
C  = 已明确补偿订单数
SR = Redis剩余库存
SM = MySQL剩余库存
```

正常无补偿：

```text
A = O
SR = S0 - A
SM = S0 - O
```

存在明确补偿：

```text
O = A - C
SR = S0 - A + C
SM = S0 - O
```

并且：

```text
超卖数 = max(0, O - S0) = 0
重复业务键数量 = 0
正常DLQ = 0
测试结束并排空后Ready/Unacked/Pending = 0
```

### 12.3 A/B 结果表

| 指标 | V6 Stream | V7.1 Relay+Rabbit | V7.2状态机+Rabbit |
|---|---:|---:|---:|
| 请求总数 |  |  |  |
| accepted |  |  |  |
| HTTP TPS |  |  |  |
| HTTP P95 ms |  |  |  |
| HTTP P99 ms |  |  |  |
| 消费 TPS |  |  |  |
| 端到端 P95 ms |  |  |  |
| 积压清空时间 s |  |  |  |
| 重复订单 |  |  |  |
| 超卖 |  |  |  |
| 丢失订单 |  |  |  |
| DLQ |  |  |  |
| App CPU峰值 |  |  |  |
| Rabbit CPU峰值 | N/A |  |  |
| Redis CPU峰值 |  |  |  |
| MySQL CPU峰值 |  |  |  |

每个正式场景至少执行三轮，报告中记录中位数和离散程度。第一轮通常受 JVM 预热影响，不应只挑最好的一次结果。

---

## 13. 预期效果和验收门槛

### 13.1 正确性硬门槛

任意一个不满足都不能进入下一阶段：

```text
超卖 = 0
重复订单 = 0
无故丢失订单 = 0
正常场景DLQ = 0
正常场景Active = 0（等待消费与对账完成后）
错误补偿 = 0
补偿重复加库存 = 0
```

### 13.2 性能预期

这些是验证方向，不是预先伪造的测试结论：

- V7.1 HTTP 请求主路径仍然是 Lua + Stream，入口性能应接近 V6；
- V7.1 的 Rabbit 多消费者落库和积压清空应明显快于当前单线程；
- `concurrency=4` 的消费吞吐预期高于 `concurrency=1`，但不承诺严格4倍；
- `concurrency=8` 可能受 MySQL 热点库存行和连接池限制，不一定优于4；
- V7.2 Lua 增加 Hash/ZSet 写入，并增加 Rabbit 发布调用，HTTP P95 可能略高于 V6；
- V7.2 的核心收益是可恢复投递、失败隔离和最终一致性，不是承诺入口 TPS 必然提高；
- Confirm 关闭可能提高 Publisher 吞吐，但可靠性降级，不能作为正式生产配置依据。

第一轮建议相对门槛：

```text
V7.1/V7.2 HTTP P95 不劣化超过 V6 的 30%
4消费者积压清空时间不高于1消费者的 60%
系统错误率 < 0.1%，目标为0
30分钟测试中 Ready/Pending 不持续增长
```

如果硬件较弱，可以调整绝对 TPS 目标，但不能降低正确性硬门槛。

### 13.3 阶段放行

第一阶段放行到第二阶段：

- Stage 1 全部逻辑、DLQ、故障和性能测试通过；
- 至少三轮报告稳定；
- 回滚流程演练通过；
- 没有依赖手工清消息才能恢复的隐藏步骤。

第二阶段最终放行：

- 所有状态迁移和补偿竞态测试通过；
- `report-only` 对账结果准确；
- 自动修正 CREATED 验证通过；
- 自动库存补偿最后开启，并通过重复执行/晚到消息测试；
- 故障测试证明应用在 Lua 后、发送前宕机不会静默丢单；
- 形成 V6/V7.1/V7.2 三版本完整对比报告。

---

## 14. 计划产物清单

实施测试时应形成：

```text
src/test/java/...                     单元和集成测试
src/test/jmeter/seckill.jmx           JMeter主测试计划
src/test/jmeter/seckill-status.jmx    异步状态延迟测试
target/jmeter/user-tokens.csv         测试Token，不提交真实凭证
target/jmeter-results/<version>/<run> JTL与HTML报告
docs/test-results/                    脱敏后的结果摘要与对比表
```

每次测试报告必须附：环境快照、数据初始化记录、JMeter命令、JTL/HTML位置、数据库核对SQL结果、Rabbit/Redis截图或指标导出、异常日志摘要和最终结论。

### 参考文档

- [Apache JMeter User Manual](https://jmeter.apache.org/usermanual/)
- [Apache JMeter Non-GUI Mode](https://jmeter.apache.org/usermanual/get-started.html#non_gui)
- [RabbitMQ Consumer Acknowledgements](https://www.rabbitmq.com/docs/confirms)
