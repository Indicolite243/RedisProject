-- 参数：
-- ARGV[1]：优惠券 ID
-- ARGV[2]：用户 ID
local voucherId = ARGV[1]
local userId = ARGV[2]

-- Redis 数据 Key：
-- String：seckill:stock:{voucherId}，保存秒杀库存
-- Set：seckill:order:{voucherId}，保存已经下单的用户 ID
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 1. 判断库存是否存在且大于 0
local stock = tonumber(redis.call('get', stockKey))
if (stock == nil or stock <= 0) then
    return 1
end

-- 2. 判断用户是否已经购买过该优惠券
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- 3. 预扣 Redis 库存
redis.call('incrby', stockKey, -1)

-- 4. 将用户记录到已下单集合
redis.call('sadd', orderKey, userId)

-- 0：抢购资格校验成功
return 0
