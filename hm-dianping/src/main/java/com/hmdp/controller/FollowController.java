package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;

    // ==================== 关注功能版本说明 ====================
    // V1：使用数据库完成关注、取关和是否关注判断
    // V2：把关注关系同步到Redis Set，并使用Set交集查询共同关注【当前使用】
    // 整理时间：2026-08-19，只整理顺序和版本标记，尽量保留原来的代码和注释
    // ========================================================

    // ==================== V1/V2 关注或取关（当前使用） ====================
    @PutMapping("/{id}/{isFollow}")
    public Result follwow(@PathVariable("id") Long followUserId, @PathVariable("isFollow") Boolean isFollow){
        return followService.follow(followUserId, isFollow);
    }

    // ==================== V1 判断是否关注（当前使用） ====================
    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long followUserId){
        return followService.isFollow(followUserId);
    }

    // ==================== V2 查询共同关注（当前使用） ====================
    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id){
        return followService.followCommons(id);
    }

}
