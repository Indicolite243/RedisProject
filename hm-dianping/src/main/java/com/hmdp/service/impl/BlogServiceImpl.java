package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private IUserService userService;

    @Autowired
    private IBlogService blogService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IFollowService followService;

    // ==================== 博客功能版本说明 ====================
    // V1：查询博客详情、热门博客，并补充博客发布者的信息
    // V2：使用Redis Set记录点赞用户，解决重复点赞问题（已停用）
    // V3：使用Redis ZSet记录点赞时间，并查询最早点赞的Top5用户
    // V4：使用推模式把博客写入粉丝收件箱，并使用ZSet完成Feed流滚动分页【当前新增】
    // 整理时间：2026-08-19，只整理顺序和版本标记，尽量保留原来的代码和注释
    // ========================================================

    // ==================== V1 查询博客详情（当前仍在使用） ====================
    @Override
    public Result queryBlogById(Long id) {
        //1查询blog
        Blog blog = getById(id);
        if (blog == null){
            return Result.fail("笔记不存在");
        }
        //2查询blog有关的用户
        queryBlogUser(blog);
        //3查询blog的是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    // ==================== V1 查询热门博客（当前仍在使用） ====================
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
                    this.queryBlogUser(blog);
                    this.isBlogLiked(blog);
                });
        return Result.ok(records);
    }

    //查询blog有关的用户
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    // ==================== V3 判断当前用户是否点赞（当前使用） ====================
    private void isBlogLiked(Blog blog) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        //用户未登录，不需要查询点赞状态
        if (user == null) {
            return;
        }
        Long userId = user.getId();
        //判断是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    // ==================== V3 Redis ZSet点赞（当前使用） ====================
    // 升级了什么：在V2判断是否点赞的基础上，用点赞时间作为score保存到ZSet
    // 优化效果：既能判断用户是否点赞，又能按照点赞时间查询Top5用户
    @Override
    public Result likeBlog(Long id) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return Result.fail("未登录，请登录");
        }
        Long userId = user.getId();
        //判断是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null){
            //未点赞可以点赞
            //数据库点赞数+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //保存用户到Redis的ZSet集合，score是点赞时间
            if (isSuccess){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }else {
            //如果已经点赞，则不能重复点赞
            //数据库点赞数-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //把用户从Redis的ZSet集合中移除
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }
        return Result.ok();
    }

    // ==================== V3 查询Top5点赞用户（当前使用） ====================
    //查询top 5点赞用户
    @Override
    public Result queryBlogLikes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        //查询top 5 zrange key 0 4
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //解析用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idstr = StrUtil.join(",", ids);
        //根据用户id查询用户 where id in (5,1) order by field(id,5,1)
        //ORDER BY FIELD必须保留，否则数据库返回的用户顺序可能与Redis中的Top5顺序不一致
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + idstr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回
        return Result.ok(userDTOS);
    }

    // ==================== V4 推模式发布博客（当前使用） ====================
    // 升级了什么：V1只保存博客，V4保存成功后还会把博客id推送到所有粉丝的收件箱
    // 收件箱结构：key为feed:粉丝id，member为博客id，score为发布时间戳
    @Override
    public Result saveBlog(Blog blog) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //保存博客
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("发布失败");
        }
        //查询博客作者粉丝 select * from tb_follow where follow_user_id=?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //推送笔记id给所有粉丝
        for (Follow follow : follows) {
            //获得粉丝id
            Long userId = follow.getUserId();
            //推送
            String key = RedisConstants.FEED_KEY + userId;
            stringRedisTemplate.opsForZSet()
                    .add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        //返回id
        return Result.ok(blog.getId());
    }

    // ==================== V4 ZSet滚动分页查询Feed（当前使用） ====================
    // max：第一次是当前时间，后续是上一页的最小时间戳
    // offset：上一页中已经读取过、并且score等于最小时间戳的数据总数
    @Override
    public Result queryBlogOfFollow(long max, Integer offset) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //查询收件箱   ZREVRANGEBYSCORE key Max Min  Limit offset count
        String key = RedisConstants.FEED_KEY + userId;
        //解析数据 blogId、score时间戳minTime、offset
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        //非空判断
        if (typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        //解析数据blogId minTime offset
        ArrayList<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 0;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //获取id
            String idStr = typedTuple.getValue();
            ids.add(Long.valueOf(idStr));
            //分数也就是时间戳
            long time = typedTuple.getScore().longValue();
            //计算本页中已经读取过、并且与最小时间戳相同的数据总数
            os = time == minTime ? os + 1 : 1;
            //更新最小时间
            minTime = time;
        }
        //根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            //查询blog有关的用户
            queryBlogUser(blog);
            //查询blog的是否被点赞
            isBlogLiked(blog);
        }
        //封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setMinTime(minTime);
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }

    /*
     * ==================== V2 Redis Set点赞版（已停用） ====================
     * 升级了什么：使用Redis Set保存点赞用户，可以判断用户是否重复点赞，也可以取消点赞
     * 为什么停用：Set只能判断成员是否存在，不能记录点赞时间，也就不能实现Top5点赞用户排序
     * V3把Set升级为ZSet，member仍然是userId，score使用点赞时间
     *
     * public Result likeBlog(Long id) {
     *     //获取当前用户
     *     Long userId = UserHolder.getUser().getId();
     *     //判断是否已经点赞
     *     String key = RedisConstants.BLOG_LIKED_KEY + id;
     *     Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
     *     if (BooleanUtil.isFalse(isMember)) {
     *         //未点赞，可以点赞
     *         //数据库点赞数+1
     *         boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
     *         //保存用户到Redis的Set集合
     *         if (isSuccess) {
     *             stringRedisTemplate.opsForSet().add(key, userId.toString());
     *         }
     *     } else {
     *         //如果已经点赞，取消点赞
     *         //数据库点赞数-1
     *         boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
     *         //把用户从Redis的Set集合中移除
     *         if (isSuccess) {
     *             stringRedisTemplate.opsForSet().remove(key, userId.toString());
     *         }
     *     }
     *     return Result.ok();
     * }
     * ========================================================================
     */
}
