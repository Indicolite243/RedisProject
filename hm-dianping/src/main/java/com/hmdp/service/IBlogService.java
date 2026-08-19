package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    // ==================== V1 博客查询 ====================
    Result queryBlogById(Long id);

    Result queryHotBlog(Integer current);

    // ==================== V2/V3 博客点赞 ====================
    Result likeBlog(Long id);

    // ==================== V3 点赞排行榜 ====================
    Result queryBlogLikes(Long id);

    // ==================== V4 Feed流推送和滚动分页 ====================
    Result saveBlog(Blog blog);

    Result queryBlogOfFollow(long max, Integer offset);
}
