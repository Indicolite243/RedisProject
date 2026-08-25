-- RabbitMQ 秒杀链路的 Redis 预占脚本。
--
-- 目标：请求线程不直接访问 RabbitMQ 或 MySQL，只在 Redis 中原子完成：
--   1. 校验库存；
--   2. 校验一人一单；
--   3. 预扣 Redis 库存；
--   4. 保存用户与订单的映射；
--   5. 建立可恢复的 Reservation；
--   6. 把 orderId 加入 Dispatcher 扫描的 Pending ZSet。
--
-- Redis 会把整份 Lua 当作一个不可分割的操作执行。脚本执行期间不会有其他命令
-- 插入这些步骤，因此不会出现“库存扣了但 Reservation 没写入”之类的中间状态。
--
-- 本脚本不使用 KEYS，所有业务参数由 Java 按以下顺序放入 ARGV：
--   ARGV[1] voucherId：优惠券 ID
--   ARGV[2] userId：用户 ID
--   ARGV[3] orderId：全局订单 ID
--   ARGV[4] messageId：稳定的业务消息 ID
--   ARGV[5] createdAt：请求受理时间戳（毫秒）
--
-- 返回值：0=预占成功，1=库存不足或库存 Key 不存在，2=该用户已经预占过该优惠券。

-- 读取 Java 传入的业务参数。
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local messageId = ARGV[4]
local createdAt = ARGV[5]

-- 根据业务 ID 拼出本次操作涉及的 Redis Key。
-- stockKey：String，剩余 Redis 库存。
-- orderMapKey：Hash，field=userId，value=orderId，用于一人一单判断。
-- reservationKey：Hash，保存这笔订单从 PREPARED 到最终状态的全过程。
-- pendingKey：ZSet，member=orderId，score=下一次允许 Dispatcher 发送的时间。
local stockKey = 'seckill:stock:' .. voucherId
local orderMapKey = 'seckill:order-map:' .. voucherId
local reservationKey = 'seckill:reservation:' .. orderId
local pendingKey = 'seckill:publish:pending'

-- 1. 检查 Redis 库存。Key 不存在和库存为 0 都按库存不足处理。
local stock = tonumber(redis.call('get', stockKey))
if stock == nil or stock <= 0 then
    return 1
end

-- 2. 检查一人一单映射。只要 userId 已经存在，就拒绝生成第二笔预占订单。
if redis.call('hexists', orderMapKey, userId) == 1 then
    return 2
end

-- 3. 预扣一张 Redis 库存。后续如果订单最终失败，只能通过补偿脚本恢复。
redis.call('incrby', stockKey, -1)

-- 4. 记录 userId 对应的 orderId，既防止重复抢购，也方便查询用户本次的订单。
redis.call('hset', orderMapKey, userId, orderId)

-- 5. 保存订单预占记录。
-- PREPARED 表示 Redis 已接受订单，但 Dispatcher 尚未确认消息进入 RabbitMQ。
-- publishAttempts 从 0 开始；nextRetryAt 使用 createdAt，使订单可被立即扫描。
redis.call(
        'hset',
        reservationKey,
        'orderId', orderId,
        'messageId', messageId,
        'userId', userId,
        'voucherId', voucherId,
        'status', 'PREPARED',
        'publishAttempts', '0',
        'nextRetryAt', createdAt,
        'createdAt', createdAt
)

-- 6. 加入待发送 ZSet。
-- score 使用毫秒时间戳，Dispatcher 查询 score <= 当前时间的订单进行发布。
redis.call('zadd', pendingKey, createdAt, orderId)

-- 前面的写操作全部完成才返回成功。
return 0
