local reservationKey = KEYS[1]
local pendingKey = KEYS[2]
local stockKey = KEYS[3]
local orderMapKey = KEYS[4]

local orderId = ARGV[1]
local action = ARGV[2]
local nextRetryAt = ARGV[3]
local maxAttempts = tonumber(ARGV[4])

local status = redis.call('hget', reservationKey, 'status')

-- Reservation不存在，清理孤立pending记录
if not status then
    redis.call('zrem', pendingKey, orderId)
    return 0
end

-- RabbitMQ发布成功
if action == 'PUBLISHED' then

    -- 防止CREATED回退成PUBLISHED
    if status == 'CREATED' or status == 'PUBLISHED' then
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    if status == 'PREPARED'
            or status == 'PUBLISHING'
            or status == 'PUBLISH_RETRY' then

        redis.call('hset', reservationKey, 'status', 'PUBLISHED')
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    return 0
end

-- RabbitMQ发布失败，安排重试
if action == 'RETRY' then

    -- 这些状态不能退回重试状态
    if status == 'PUBLISHED'
            or status == 'CREATED'
            or status == 'DEAD'
            or status == 'COMPENSATING'
            or status == 'COMPENSATED' then

        redis.call('zrem', pendingKey, orderId)
        return 0
    end

    local attempts = redis.call(
            'hincrby',
            reservationKey,
            'publishAttempts',
            1
    )
            -- 达到最大重试次数，停止继续投递
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

    redis.call(
            'hset',
            reservationKey,
            'status', 'PUBLISH_RETRY',
            'nextRetryAt', nextRetryAt
    )

    redis.call('zadd', pendingKey, nextRetryAt, orderId)
    return 1
end

-- MySQL订单已经创建成功
if action == 'CREATED' then

    -- 已经完成补偿时，不允许重新变成CREATED
    if status == 'COMPENSATING'
            or status == 'COMPENSATED' then
        return -1
    end

    redis.call(
            'hset',
            reservationKey,
            'status', 'CREATED',
            'nextRetryAt', '0'
    )

    redis.call('zrem', pendingKey, orderId)
    return 1
end

-- 人工重新投递DEAD订单
if action == 'REQUEUE' then

    -- 只有已经停止重试的DEAD订单允许重新投递
    if status ~= 'DEAD' then
        return 0
    end

    -- 恢复成等待发布状态，并重新获得5次发布机会
    redis.call(
            'hset',
            reservationKey,
            'status', 'PUBLISH_RETRY',
            'publishAttempts', '0',
            'nextRetryAt', nextRetryAt
    )

    -- 重新放入待发布ZSet
    redis.call('zadd', pendingKey, nextRetryAt, orderId)
    return 1
end

-- 人工补偿DEAD订单
if action == 'COMPENSATE' then

    -- 已经补偿完成时直接返回，不能重复增加库存
    if status == 'COMPENSATED' then
        redis.call('zrem', pendingKey, orderId)
        return 1
    end

    -- 只有彻底停止投递的DEAD订单才能补偿
    if status ~= 'DEAD' then
        return 0
    end

    local userId = redis.call(
            'hget',
            reservationKey,
            'userId'
    )

    if not userId then
        return -2
    end

    -- 确认用户的一人一单记录仍然属于当前订单
    local mappedOrderId = redis.call(
            'hget',
            orderMapKey,
            userId
    )

    if mappedOrderId ~= orderId then
        return -2
    end

    -- 库存Key必须存在，并且当前值必须是数字
    local stock = tonumber(redis.call('get', stockKey))

    if not stock then
        return -3
    end

    -- 以下修改在同一个Lua脚本中原子执行
    redis.call(
            'hset',
            reservationKey,
            'status',
            'COMPENSATING'
    )

    -- 恢复一张Redis库存
    redis.call('incrby', stockKey, 1)

    -- 删除该用户的一人一单资格记录
    redis.call('hdel', orderMapKey, userId)

    -- 确保订单不再等待发布
    redis.call('zrem', pendingKey, orderId)

    -- 标记补偿完成
    redis.call(
            'hset',
            reservationKey,
            'status', 'COMPENSATED',
            'nextRetryAt', '0'
    )

    return 1
end

-- 不认识的action
return -1
