package com.hmdp.controller;


import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.service.IBlogService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/blog")
public class BlogController {

    @Resource
    private IBlogService blogService;

    // ==================== 博客功能版本说明 ====================
    // V1：完成发布博客、查询我的博客、热门博客和博客详情
    // V2：使用Redis Set判断用户是否已经点赞（已停用）
    // V3：使用Redis ZSet记录点赞时间，并查询最早点赞的Top5用户
    // V4：发布博客时推送到粉丝收件箱，并使用ZSet完成Feed流滚动分页【当前新增】
    // 整理时间：2026-08-19，只整理顺序和版本标记，尽量保留原来的代码和注释
    // ========================================================

    // ==================== V4 发布博客并推送到粉丝收件箱（当前使用） ====================
    @PostMapping
    public Result saveBlog(@RequestBody Blog blog) {
        return blogService.saveBlog(blog);
    }

    // ==================== V1 查询我的博客（当前仍在使用） ====================
    @GetMapping("/of/me")
    public Result queryMyBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    // ==================== V1 查询热门博客（当前仍在使用） ====================
    @GetMapping("/hot")
    public Result queryHotBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        return blogService.queryHotBlog(current);
    }

    // ==================== V1 查询博客详情（当前仍在使用） ====================
    @GetMapping("/{id}")
    public Result queryBlogById(@PathVariable("id") Long id) {
        return blogService.queryBlogById(id);
    }

    // ==================== V3 点赞或取消点赞（当前使用） ====================
    @PutMapping("/like/{id}")
    public Result likeBlog(@PathVariable("id") Long id) {
        // ==================== V1 直接修改数据库点赞数（已停用） ====================
        // 实现了什么：每次请求直接让数据库点赞数+1
        // 为什么停用：不能判断用户是否重复点赞，也不能完成取消点赞
//        // 修改点赞数量 update blog set liked = liked + 1 where id = ?
//        blogService.update()
//                .setSql("liked = liked + 1").eq("id", id).update();
//        return Result.ok();

        // V2和V3把点赞判断放到Service中完成
        return blogService.likeBlog(id);
    }

    // ==================== V3 查询最早点赞的Top5用户（当前使用） ====================
    @GetMapping("/likes/{id}")
    public Result queryBlogLikes(@PathVariable("id") Long id) {
        return blogService.queryBlogLikes(id);
    }

    // ==================== V1 查询指定用户的博客（当前仍在使用） ====================
    @GetMapping("/of/user")
    public Result queryBlogByUserId(
            @RequestParam(value = "current", defaultValue = "1") Integer current,
            @RequestParam("id") Long id) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    // ==================== V4 滚动分页查询关注Feed（当前使用） ====================
    @GetMapping("/of/follow")
    public Result queryBlogOfFollow(@RequestParam("lastId") long max,
                                    @RequestParam(value = "offset", defaultValue = "0") Integer offset){
        return blogService.queryBlogOfFollow(max, offset);
    }
}
