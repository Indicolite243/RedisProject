# RedisProject 会话命令整理

本文整理截图中 RedisProject 相关会话里出现过、讨论过或用于排障的命令。命令已按场景去重，并移除了 Codex 自身的运行包装命令。

> 使用约定
>
> - `<redis-host>`、`<password>`、`<voucherId>`、`<pid>` 等必须替换成真实值。
> - Redis 交互命令需要先进入 `redis-cli`；Shell 命令直接在 PowerShell、CMD 或 Linux 终端执行。
> - 标有“危险”的命令会删除数据、停止服务或删除容器，执行前必须确认目标。

## 1. Java、JDK 与 Maven

### 1.1 检查当前 Java/Maven 环境

Windows PowerShell：

```powershell
java -version
javac -version
mvn -version
where.exe java
where.exe javac
$env:JAVA_HOME
Get-Command java | Select-Object -ExpandProperty Source
Get-Command mvn | Select-Object -ExpandProperty Source
```

用途：确认 IDEA、终端和 Maven 实际使用的是哪一个 JDK。项目 `pom.xml` 目前声明 Java 8，因此看到 JDK 21 并不代表 Maven 测试一定在使用 Java 8。

仅对当前 PowerShell 临时切换 JDK：

```powershell
$env:JAVA_HOME = "C:\Program Files\Amazon Corretto\jdk1.8.0_xxx"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
java -version
mvn -version
```

关闭当前终端后，临时修改失效。

### 1.2 编译和测试项目

```powershell
cd "D:\SeekJob\hm-dianping\hm-dianping"
mvn -DskipTests compile
mvn test
mvn -Dtest=HmDianPingApplicationTests test
mvn -Dtest=HmDianPingApplicationTests#generateJmeterUserTokens test
```

常用含义：

| 命令 | 用途 |
|---|---|
| `mvn -DskipTests compile` | 只编译，不运行测试 |
| `mvn test` | 编译并运行全部测试 |
| `mvn -Dtest=类名 test` | 只运行指定测试类 |
| `mvn -Dtest=类名#方法名 test` | 只运行指定测试方法 |

### 1.3 检查 Maven 依赖和 Spring AMQP 版本

```powershell
mvn dependency:tree
mvn dependency:tree "-Dincludes=org.redisson:redisson"
mvn help:evaluate "-Dexpression=spring-amqp.version" -q -DforceStdout
mvn help:evaluate "-Dexpression=spring-retry.version" -q -DforceStdout
mvn help:effective-pom | Select-String -Pattern "spring-amqp|spring-rabbit|spring-retry" -Context 1,2
```

下载指定依赖进行兼容性检查：

```powershell
mvn dependency:get "-Dartifact=org.springframework.amqp:spring-rabbit:2.2.18.RELEASE" -q
```

`javap` 用于检查某个依赖版本实际提供的方法，属于高级排障命令：

```powershell
javap -classpath "<jar-path>" "org.springframework.amqp.rabbit.connection.CorrelationData"
javap -classpath "<jar-path>" "org.springframework.amqp.rabbit.core.RabbitOperations"
```

## 2. Spring Boot 启动与端口排障

### 2.1 启动项目

```powershell
cd "D:\SeekJob\hm-dianping\hm-dianping"
mvn spring-boot:run
```

或者先打包再运行：

```powershell
mvn clean package -DskipTests
java -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar
```

### 2.2 检查 8081 端口和 Java 进程

CMD：

```bat
netstat -ano | findstr :8081
tasklist /FI "PID eq <pid>"
```

PowerShell：

```powershell
Get-NetTCPConnection -LocalPort 8081 -ErrorAction SilentlyContinue
Get-Process -Id <pid>
```

只有确认 PID 属于需要结束的旧项目进程后，才执行：

```powershell
Stop-Process -Id <pid>
```

强制停止属于高风险操作：

```bat
taskkill /PID <pid> /F
```

## 3. Redis 连接、认证与运行配置

### 3.1 连接 Redis

本机默认实例：

```bash
redis-cli
```

指定主机、端口：

```bash
redis-cli -h <redis-host> -p 6379
```

进入客户端后认证和测试：

```redis
AUTH <password>
PING
```

应分别返回 `OK` 和 `PONG`。

不建议把真实密码直接写在 `redis-cli -a <password>` 中，因为它可能出现在命令历史或进程列表里。如果只在受控学习环境中临时使用：

```bash
redis-cli -h <redis-host> -p 6379 -a <password>
```

### 3.2 查看 Redis 当前生效配置

```redis
CONFIG GET bind
CONFIG GET requirepass
CONFIG GET protected-mode
CONFIG GET port
CONFIG GET *
```

查看服务信息：

```redis
INFO server
INFO clients
INFO memory
INFO persistence
INFO replication
```

### 3.3 查找 Redis 配置文件和启动参数

Linux：

```bash
find / -name redis.conf 2>/dev/null
ps -ef | grep redis-server
cat /etc/redis/redis.conf
less /etc/redis/redis.conf
```

在 `less` 中可用 `/requirepass`、`/bind`、`/protected-mode` 搜索，按 `q` 退出。

## 4. Redis 主从复制（7001/7002/7003）

### 4.1 从库配置

Redis 6.2 推荐在 7002、7003 的 `redis.conf` 中配置：

```conf
replicaof 127.0.0.1 7001
```

`slaveof` 是兼容旧版本的旧名称，当前使用 `replicaof`。

### 4.2 启动三个实例

在三个 Linux 终端分别执行：

```bash
redis-server /tmp/7001/redis.conf
redis-server /tmp/7002/redis.conf
redis-server /tmp/7003/redis.conf
```

注意：`/tmp` 可能在重启或系统清理后丢失，正式环境应把配置迁到 `/etc/redis/`，数据迁到 `/var/lib/redis/`，并配置 systemd 服务。

### 4.3 验证主从状态

```bash
redis-cli -p 7001 ping
redis-cli -p 7001 info replication
redis-cli -p 7002 info replication
redis-cli -p 7003 info replication
```

重点检查：

```text
7001: role:master、connected_slaves:2
7002/7003: role:slave、master_link_status:up
```

写入主库并从两个从库读取：

```bash
redis-cli -p 7001 set replication:test ok
redis-cli -p 7002 get replication:test
redis-cli -p 7003 get replication:test
```

### 4.4 安全停止实例

以下命令会停止 Redis 服务：

```bash
redis-cli -p 7003 shutdown save
redis-cli -p 7002 shutdown save
redis-cli -p 7001 shutdown save
```

应先停从库、最后停主库。若端口提示 `Address already in use`，先用 `PING` 和 `INFO replication` 判断实例是否已经运行，不要重复启动。

## 5. Redis 业务数据排查

### 5.1 通用 Key 检查

```redis
EXISTS <key>
TYPE <key>
TTL <key>
PTTL <key>
```

TTL 返回值：

| 返回值 | 含义 |
|---:|---|
| 正数 | 剩余有效期，`TTL` 单位秒，`PTTL` 单位毫秒 |
| `-1` | Key 存在，但没有过期时间 |
| `-2` | Key 不存在 |

生产或大数据量环境优先使用渐进式扫描：

```redis
SCAN 0 MATCH <pattern> COUNT 100
```

不要在大库中使用 `KEYS *`。

删除 Key（危险）：

```redis
DEL <key>
```

### 5.2 验证码和登录 Token

验证码是 String：

```redis
GET login:code:<phone>
TTL login:code:<phone>
```

登录用户是 Hash：

```redis
TYPE login:token:<token>
HGETALL login:token:<token>
TTL login:token:<token>
EXPIRE login:token:<token> 1800
```

业务含义：用户每次携带有效 Token 请求时，项目调用 `EXPIRE` 重新设置 TTL，形成滑动过期。

注销或测试清理 Token（危险）：

```redis
DEL login:token:<token>
```

### 5.3 店铺缓存与缓存 TTL

```redis
TYPE cache:shop:<shopId>
GET cache:shop:<shopId>
TTL cache:shop:<shopId>
```

带物理过期时间写缓存：

```redis
SET cache:shop:<shopId> '<json>' EX 1800
```

缓存没有设置过期时间时，`TTL` 会返回 `-1`。逻辑过期方案可能故意不设置 Redis TTL，而是把业务过期时间放进 JSON；不能只看到 `-1` 就直接判断代码错误。

### 5.4 分布式锁

加锁的核心原生命令：

```redis
SET lock:<business-key> <owner-id> NX EX 10
```

检查锁：

```redis
GET lock:<business-key>
PTTL lock:<business-key>
```

释放锁不能简单地无条件 `DEL`，应通过 Lua 原子完成“判断 owner-id + 删除”：

```lua
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
```

手动删除锁仅用于确认没有业务线程持锁的测试环境：

```redis
DEL lock:<business-key>
```

### 5.5 全局唯一 ID

项目使用 Redis 自增序列：

```redis
INCR icr:order:<yyyy:MM:dd>
GET icr:order:<yyyy:MM:dd>
```

注意：当前项目代码中的前缀确实是 `icr:`，不是常见拼写 `incr:`。不要在排查时误查另一个 Key。

### 5.6 秒杀库存和一人一单

```redis
GET seckill:stock:<voucherId>
EXISTS seckill:stock:<voucherId>
SISMEMBER seckill:order:<voucherId> <userId>
SCARD seckill:order:<voucherId>
SCAN 0 MATCH seckill:* COUNT 100
```

秒杀 Lua 中对应的核心命令：

```redis
GET seckill:stock:<voucherId>
SISMEMBER seckill:order:<voucherId> <userId>
INCRBY seckill:stock:<voucherId> -1
SADD seckill:order:<voucherId> <userId>
XADD stream.order * userId <userId> voucherId <voucherId> id <orderId>
```

这些动作在项目的 `seckill.lua` 中一次原子执行，手动逐条执行只能用于理解，不能替代生产业务脚本。

### 5.7 博客点赞和 Feed 流

博客点赞使用 ZSet：

```redis
TYPE blog:liked:<blogId>
ZSCORE blog:liked:<blogId> <userId>
ZRANGE blog:liked:<blogId> 0 4 WITHSCORES
```

Feed 收件箱同样使用 ZSet：

```redis
ZREVRANGEBYSCORE feed:<userId> <maxTimestamp> 0 WITHSCORES LIMIT <offset> <count>
```

清理错误类型或测试数据（危险）：

```redis
DEL blog:liked:<blogId>
DEL feed:<userId>
```

### 5.8 签到 Bitmap

```redis
SETBIT sign:<userId>:<yyyyMM> <dayOfMonth-1> 1
GETBIT sign:<userId>:<yyyyMM> <dayOfMonth-1>
BITFIELD sign:<userId>:<yyyyMM> GET u<dayOfMonth> 0
```

接口测试：

```bat
curl.exe -X POST "http://localhost:8081/user/sign" -H "authorization: <token>"
curl.exe -X POST "http://localhost:8081/user/sign/count" -H "authorization: <token>"
```

## 6. Redis Stream 秒杀队列

### 6.1 创建消费者组

只需创建一次：

```redis
XGROUP CREATE stream.order g1 0 MKSTREAM
```

如果组已经存在，会返回 `BUSYGROUP`，这不代表 Stream 损坏。项目使用的是 `stream.order`，不要误写成 `stream.orders`。

### 6.2 查看 Stream 和消费者组

```redis
XLEN stream.order
XRANGE stream.order - + COUNT 10
XINFO GROUPS stream.order
XINFO CONSUMERS stream.order g1
XPENDING stream.order g1
```

### 6.3 模拟消费者读取与确认

读取新消息：

```redis
XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.order >
```

读取当前消费者 Pending List 中未确认的旧消息：

```redis
XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.order 0
```

处理成功后确认：

```redis
XACK stream.order g1 <message-id>
```

清空整个 Stream（危险，会删除消息和消费者组）：

```redis
DEL stream.order
```

清空后若项目仍使用 Stream，需要重新执行 `XGROUP CREATE ... MKSTREAM`。

## 7. RabbitMQ 与 Docker

截图中的会话后续还包含 RabbitMQ 学习和 RedisProject 升级讨论，主要命令如下。

### 7.1 Docker 服务和容器

```bash
systemctl start docker
systemctl enable docker
systemctl restart docker
docker ps
docker start <rabbit-container>
docker stop <rabbit-container>
docker pause <rabbit-container>
docker unpause <rabbit-container>
docker update --restart=unless-stopped <rabbit-container>
```

`docker restart`、`stop`、`pause` 会中断消息服务，压测和生产环境执行前必须确认影响。

删除容器（危险，容器内未挂载的数据可能丢失）：

```bash
docker rm <rabbit-container>
```

### 7.2 RabbitMQ 延迟消息插件

查看插件目录和安装状态：

```bash
docker exec <rabbit-container> rabbitmq-plugins directories -s
docker exec <rabbit-container> rabbitmq-plugins list
docker exec <rabbit-container> rabbitmq-plugins list | grep delayed
```

把插件复制进容器：

```powershell
docker cp "<local-path>\rabbitmq_delayed_message_exchange-<version>.ez" <rabbit-container>:/opt/rabbitmq/plugins/
```

启用并复查：

```bash
docker exec <rabbit-container> rabbitmq-plugins enable rabbitmq_delayed_message_exchange
docker exec <rabbit-container> rabbitmq-plugins list | grep delayed
```

进入容器排查：

```bash
docker exec -it <rabbit-container> bash
cd /opt/rabbitmq/plugins
```

插件版本必须与 RabbitMQ 版本兼容，不能只看文件名直接安装。

## 8. JMeter 与 HTTP 接口测试

### 8.1 启动 JMeter GUI

```powershell
cd "<jmeter-home>\bin"
.\jmeter.bat
```

GUI 适合编写、调试 `.jmx`，正式压测使用非 GUI 模式。

### 8.2 非 GUI 压测并生成报告

```powershell
.\jmeter.bat -n -t .\test.jmx -l .\result.jtl -e -o .\report
```

参数含义：

| 参数 | 用途 |
|---|---|
| `-n` | 非 GUI 模式 |
| `-t` | 指定 `.jmx` 测试计划 |
| `-l` | 保存 `.jtl` 结果 |
| `-e` | 测试结束后生成报告 |
| `-o` | 指定 HTML 报告目录，目录必须为空或不存在 |

### 8.3 常用接口验证

```bash
curl.exe "http://localhost:8081/shop/1"
curl.exe "http://localhost:8081/blog/hot?current=1"
curl.exe "http://localhost:8081/user/me" -H "authorization: <token>"
```

## 9. MySQL 数据校验

检查一人一单：

```sql
SELECT COUNT(*)
FROM tb_voucher_order
WHERE user_id = <userId>
  AND voucher_id = <voucherId>;
```

检查某张优惠券的订单总数：

```sql
SELECT COUNT(*)
FROM tb_voucher_order
WHERE voucher_id = <voucherId>;
```

检查库存：

```sql
SELECT *
FROM tb_seckill_voucher
WHERE voucher_id = <voucherId>;
```

项目使用的数据库扣库存语句：

```sql
UPDATE tb_seckill_voucher
SET stock = stock - 1
WHERE voucher_id = <voucherId>
  AND stock > 0;
```

不要为了“验证”直接手动执行这条 `UPDATE`，否则 Redis 预扣库存和 MySQL 库存会不一致。优先使用 `SELECT` 做只读检查。

## 10. Git 常用流程

### 10.1 查看状态和差异

```bash
git status
git branch --show-current
git diff
git log --oneline -8
```

### 10.2 提交指定文件

```bash
git add <file-1> <file-2>
git diff --cached
git commit -m "中文提交说明"
```

不要习惯性使用 `git add .`，先明确本次提交范围，避免把 `.idea/`、测试结果或临时文件一并提交。

### 10.3 同步和推送

```bash
git pull --rebase origin <branch>
git push -u origin <branch>
```

已有 upstream 时：

```bash
git pull --rebase
git push
```

处理 rebase 冲突：

```bash
git status
git add <resolved-file>
git rebase --continue
```

切换和新建功能分支：

```bash
git switch main
git pull --ff-only origin main
git switch -c feature/<feature-name>
```

删除分支前先确认内容已经合并：

```bash
git branch -d <local-branch>
git push origin --delete <remote-branch>
```

## 11. 最容易踩坑的命令

| 命令 | 风险或注意事项 |
|---|---|
| `DEL stream.order` | 删除整个订单 Stream 和消费者组 |
| `DEL login:token:<token>` | 让指定登录立即失效 |
| `DEL blog:liked:<id>` | 清空该博客的 Redis 点赞记录 |
| `SHUTDOWN SAVE` | 保存后停止 Redis 实例 |
| `docker rm` | 删除容器，未挂载数据可能丢失 |
| `CONFIG SET` | 动态修改 Redis 运行配置，可能与配置文件不一致 |
| `KEYS *` | Key 多时阻塞 Redis，应用环境不要使用 |
| 手动执行库存 `UPDATE` | 可能造成 Redis/MySQL 库存不一致 |
| `git push --force` | 改写远程历史，本次会话不需要使用 |

## 12. 一套最小排障顺序

遇到“项目启动了但接口或 Redis 功能不正常”时，可以依次执行：

```powershell
java -version
mvn -version
cd "D:\SeekJob\hm-dianping\hm-dianping"
mvn -DskipTests compile
netstat -ano | findstr :8081
```

然后检查 Redis：

```bash
redis-cli -h <redis-host> -p 6379
```

```redis
AUTH <password>
PING
INFO replication
SCAN 0 MATCH login:token:* COUNT 20
SCAN 0 MATCH seckill:* COUNT 100
XINFO GROUPS stream.order
```

最后再根据具体业务检查 Token、缓存、库存、订单和 Pending List。这样可以先区分问题位于 JDK/Maven、端口、Redis 连接、业务 Key，还是 Stream 消费链路。
