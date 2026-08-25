-- Reservation 状态流转与补偿脚本。
--
-- 为什么统一放在一个 Lua 中：一次状态变化经常需要同时修改 Reservation Hash
-- 和 Pending ZSet，补偿还要同步修改库存与一人一单映射。Lua 可以让校验和修改
-- 在 Redis 内原子执行，避免并发线程或应用宕机产生不一致的中间状态。
--
-- KEYS：
--   KEYS[1] reservationKey：seckill:reservation:{orderId}
--   KEYS[2] pendingKey：seckill:publish:pending
--   KEYS[3] stockKey：seckill:stock:{voucherId}，仅 COMPENSATE 使用
--   KEYS[4] orderMapKey：seckill:order-map:{voucherId}，仅 COMPENSATE 使用
--
-- ARGV：
--   ARGV[1] orderId：订单 ID，也是 Pending ZSet 的 member
--   ARGV[2] action：PUBLISHED / RETRY / CREATED / REQUEUE / COMPENSATE
--   ARGV[3] nextRetryAt：下次发送时间；不需要该参数的动作传 0 占位
--   ARGV[4] maxAttempts：最大发布次数，仅 RETRY 使用
--
-- 状态含义：
--   PREPARED      Redis 预占完成，等待首次发布
--   PUBLISH_RETRY 发布失败，等待再次发布
--   PUBLISHED     RabbitMQ 已确认接收消息
--   CREATED       MySQL 订单已经创建
--   DEAD          发布达到上限，停止自动重试
--   COMPENSATING  正在原子补偿过程中的过渡状态
--   COMPENSATED   库存和购买资格均已恢复

-- 读取通用 Key；COMPENSATE 之外的动作通常只会传入前两个 Key。
local reservationKey = KEYS[1]
local pendingKey = KEYS[2]
local stockKey = KEYS[3]
local orderMapKey = KEYS[4]

-- 读取通用参数。不存在的 ARGV 会得到 nil，但只有对应 action 才会使用它。
local orderId = ARGV[1]
local action = ARGV[2]
local nextRetryAt = ARGV[3]
local maxAttempts = tonumber(ARGV[4])

-- 后续所有迁移都以 Redis 中的当前状态为判断依据，调用方不能强行覆盖状态。
local status = redis.call('hget', reservationKey, 'status')

-- Reservation 不存在时没有可发送的业务数据，同时清理可能残留的孤立 Pending 成员。
if not status then
    redis.call('zrem', pendingKey, orderId)
    return 0
end

-- PUBLISHED：发布者已经收到 RabbitMQ Confirm ACK，并确认消息没有被 Return。
if action == 'PUBLISHED' then

    -- Confirm 回调可能晚于消费者落库完成，绝不能让 CREATED 回退成 PUBLISHED。
    -- 重复执行 PUBLISHED 也按成功处理，以保证状态更新幂等。
    if status == 'CREATED' or status == 'PUBLISHED' then
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    -- 只有仍处于发布过程的状态才允许进入 PUBLISHED。
    if status == 'PREPARED'
            or status == 'PUBLISHING'
            or status == 'PUBLISH_RETRY' then

        redis.call('hset', reservationKey, 'status', 'PUBLISHED')
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    return 0
end

-- RETRY：本次发布抛出异常、Confirm NACK、Confirm 超时或消息被 Return。
if action == 'RETRY' then

    -- 已进入后续或终止状态的订单不能退回 PUBLISH_RETRY。
    -- 同时清理 Pending，防止 Dispatcher 继续扫描无须发送的订单。
    if status == 'PUBLISHED'
            or status == 'CREATED'
            or status == 'DEAD'
            or status == 'COMPENSATING'
            or status == 'COMPENSATED' then

        redis.call('zrem', pendingKey, orderId)
        return 0
    end

    -- 原子增加发布次数，避免多个 Dispatcher 同时失败时出现计数覆盖。
    local attempts = redis.call(
            'hincrby',
            reservationKey,
            'publishAttempts',
            1
    )
    -- 达到最大次数后进入 DEAD，停止自动投递，等待人工重放或补偿。
    if attempts >= maxAttempts then
        redis.call(
                'hset',
                reservationKey,
                'status', 'DEAD',
                'nextRetryAt', '0'
        )

        redis.call('zrem', pendingKey, orderId)
        return 2
    end

    -- 尚有重试机会：记录下次可发送时间，并使用同一时间更新 ZSet score。
    redis.call(
            'hset',
            reservationKey,
            'status', 'PUBLISH_RETRY',
            'nextRetryAt', nextRetryAt
    )

    redis.call('zadd', pendingKey, nextRetryAt, orderId)
    return 1
end

-- CREATED：RabbitMQ Listener 中的 MySQL 事务已经成功提交。
if action == 'CREATED' then

    -- 补偿开始后不能再改成 CREATED，否则会出现“库存已退回但又声称订单成功”。
    if status == 'COMPENSATING'
            or status == 'COMPENSATED' then
        return -1
    end

    -- CREATED 是正常处理终态，同时删除所有可能残留的待发布索引。
    redis.call(
            'hset',
            reservationKey,
            'status', 'CREATED',
            'nextRetryAt', '0'
    )

    redis.call('zrem', pendingKey, orderId)
    return 1
end

-- REQUEUE：管理员确认消息可以再次尝试后，人工重置 DEAD 订单。
if action == 'REQUEUE' then

    -- 只有已经停止自动重试的 DEAD 订单允许人工重新投递。
    if status ~= 'DEAD' then
        return 0
    end

    -- 恢复成等待发布状态并清零历史尝试次数，重新获得完整的发布机会。
    redis.call(
            'hset',
            reservationKey,
            'status', 'PUBLISH_RETRY',
            'publishAttempts', '0',
            'nextRetryAt', nextRetryAt
    )

    -- nextRetryAt 通常由 Java 传当前时间，因此 Dispatcher 可以立即扫描到它。
    redis.call('zadd', pendingKey, nextRetryAt, orderId)
    return 1
end

-- COMPENSATE：管理员确认 MySQL 没有订单后，恢复 Redis 库存和购买资格。
if action == 'COMPENSATE' then

    -- 已经补偿完成时按成功返回，但不重复增加库存，保证接口幂等。
    if status == 'COMPENSATED' then
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    -- 只有彻底停止投递的 DEAD 订单才能补偿；发布中的订单不能提前退库存。
    if status ~= 'DEAD' then
        return 0
    end

    -- Reservation 中必须能找到原始 userId，后面要用它校验并删除一人一单映射。
    local userId = redis.call(
            'hget',
            reservationKey,
            'userId'
    )

    if not userId then
        return -2
    end

    -- 一人一单映射必须仍然指向本 orderId，避免误删用户后来创建的其他订单映射。
    local mappedOrderId = redis.call(
            'hget',
            orderMapKey,
            userId
    )

    if mappedOrderId ~= orderId then
        return -2
    end

    -- 库存 Key 必须存在且值可转为数字，否则停止补偿，防止把异常数据继续扩大。
    local stock = tonumber(redis.call('get', stockKey))

    if not stock then
        return -3
    end

    -- 以下状态、库存、映射和 ZSet 修改在同一次 Lua 执行中原子完成。
    -- COMPENSATING 是过程标记；其他代码看到它时不会再落库或再次补偿。
    redis.call(
            'hset',
            reservationKey,
            'status',
            'COMPENSATING'
    )

    -- 恢复一张 Redis 库存，对应预占脚本中的一次 -1。
    redis.call('incrby', stockKey, 1)

    -- 删除该用户的一人一单映射，补偿完成后用户才可以重新抢购。
    redis.call('hdel', orderMapKey, userId)

    -- 确保订单不再等待发布，防止补偿后 Dispatcher 又把它发到 RabbitMQ。
    redis.call('zrem', pendingKey, orderId)

    -- 最后标记补偿完成。重复调用时会命中上面的幂等分支。
    redis.call(
            'hset',
            reservationKey,
            'status', 'COMPENSATED',
            'nextRetryAt', '0'
    )

    return 1
end

-- 未知 action 属于调用方错误，返回 -1，不修改任何业务数据。
return -1
