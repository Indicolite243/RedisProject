package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IFollowService extends IService<Follow> {

    // ==================== V1/V2 关注基础功能 ====================
    Result follow(Long followUserId, Boolean isFollow);

    Result isFollow(Long followUserId);

    // ==================== V2 Redis共同关注 ====================
    Result followCommons(Long id);
}
