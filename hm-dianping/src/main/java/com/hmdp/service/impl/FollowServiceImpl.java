package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IUserService userService;

    // ==================== 关注功能版本说明 ====================
    // V1：关注关系只保存到数据库，使用数据库判断是否关注（已升级）
    // V2：数据库保存成功后同步写入Redis Set，并使用Set交集查询共同关注【当前使用】
    // 整理时间：2026-08-19，只整理顺序和版本标记，尽量保留原来的代码和注释
    // ========================================================

    // ==================== V2 关注或取关并同步Redis（当前使用） ====================
    // 升级了什么：V1完成数据库操作后，把关注用户id同步保存到Redis Set
    // 优化效果：共同关注可以直接使用Redis的SINTER命令求交集
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取登录 用户
        Long userId = UserHolder.getUser().getId();

        //判断关注还是取关
        if (isFollow){
            //关注 新增关注数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess){
                //把关注用户的id放入redis的set集合 sadd user:follows:1 2 3
                String key = RedisConstants.FOLLOWS_KEY + userId;
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }else {
            //取关 删除关注数据 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));

            //把关注用户的id从redis的set集合移除
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(
                        RedisConstants.FOLLOWS_KEY + userId, followUserId.toString());
            }
        }
        return Result.ok();
    }

    // ==================== V1 使用数据库判断是否关注（当前仍在使用） ====================
    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        //1.查询是否关注 select count(*)  from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        //判断是否关注
        return Result.ok(count > 0);
    }

    // ==================== V2 Redis Set查询共同关注（当前使用） ====================
    // 当前用户的关注Set 与 对方用户的关注Set 求交集，交集中的用户就是共同关注
    @Override
    public Result followCommons(Long id) {
        //先获取当前 用户
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FOLLOWS_KEY + userId;
        //求交集
        String key2 = RedisConstants.FOLLOWS_KEY + id;
        Set<String> intersect = stringRedisTemplate.opsForSet()
                .intersect(key, key2);
        if (intersect == null || intersect.isEmpty()){
            //无交集 返回空
            return Result.ok(Collections.emptyList());
        }
        //解析id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询 用户
        List<UserDTO> users = userService.listByIds(ids).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }

    /*
     * ==================== V1 只操作数据库版（已升级） ====================
     * 实现了什么：在tb_follow表中新增或删除关注关系，可以完成关注、取关和是否关注判断
     * 为什么升级：共同关注需要频繁计算两个人关注列表的交集，直接查询数据库不方便
     * V2保留V1的数据库操作，并在数据库操作成功后同步维护Redis Set
     *
     * public Result follow(Long followUserId, Boolean isFollow) {
     *     //获取登录 用户
     *     Long userId = UserHolder.getUser().getId();
     *
     *     //判断关注还是取关
     *     if (isFollow) {
     *         //关注 新增关注数据
     *         Follow follow = new Follow();
     *         follow.setUserId(userId);
     *         follow.setFollowUserId(followUserId);
     *         save(follow);
     *     } else {
     *         //取关 删除关注数据 delete from tb_follow where user_id = ? and follow_user_id = ?
     *         remove(new QueryWrapper<Follow>()
     *                 .eq("user_id", userId).eq("follow_user_id", followUserId));
     *     }
     *     return Result.ok();
     * }
     * ========================================================================
     */
}
