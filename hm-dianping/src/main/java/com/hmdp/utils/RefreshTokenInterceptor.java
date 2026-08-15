package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Token 刷新拦截器。
 *
 * <p>该拦截器会拦截所有请求，但它本身不负责强制拦截未登录请求。
 * 它的主要职责是：尝试从请求头中获取 Token，
 * 根据 Token 从 Redis 中查询用户信息，将用户保存到 ThreadLocal，
 * 并在用户存在时刷新 Token 的有效期。</p>
 *
 * <p>当前项目中该拦截器的执行顺序为 order(0)，
 * LoginInterceptor 的执行顺序为 order(1)。
 * 因此需要先由本拦截器完成用户识别，
 * 后面的 LoginInterceptor 才能根据 UserHolder 判断当前接口是否需要登录。</p>
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    /**
     * Redis 操作模板，用于查询用户信息和刷新 Token 对应的过期时间。
     */
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 构造拦截器，并注入 Redis 操作模板。
     *
     * @param stringRedisTemplate Redis 字符串操作模板
     */
    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 从请求头中获取前端携带的 Token。
        // 当前项目约定 authorization 请求头中直接保存 Token，暂不包含 "Bearer " 前缀。
        String token = request.getHeader("authorization");

        // 没有携带 Token 时不在这里拦截请求。
        // 因为当前请求可能是登录、发送验证码或其他公开接口，
        // 是否必须登录由后面的 LoginInterceptor 决定。
        if (StrUtil.isBlank(token)) {
            return true;
        }

        // 根据 Token 拼接 Redis 中保存用户信息的 key。
        // 登录成功时保存的 key 格式为：login:token:{token}
        String key = RedisConstants.LOGIN_USER_KEY + token;

        // 从 Redis Hash 中查询当前 Token 对应的用户信息。
        // 如果 Token 已过期，Redis 中的 key 会被删除，此处会得到空 Map。
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);

        // Redis 中没有找到用户时，说明 Token 无效或已经过期。
        // 这里仍然返回 true，不直接返回 401：
        // - 公开接口可以继续访问；
        // - 受保护接口交给 LoginInterceptor 统一判断并返回 401。
        if (userMap.isEmpty()) {
            return true;
        }

        // 将 Redis Hash 中的字段转换为 UserDTO。
        // UserDTO 是当前请求中传递用户身份信息的对象，不需要再次查询数据库。
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        // 将当前用户保存到 ThreadLocal。
        // 后续的 LoginInterceptor 和 Controller 可以通过 UserHolder 获取当前用户。
        // ThreadLocal 只在当前请求线程中有效，不会作为持久化数据保存。
        UserHolder.saveUser(userDTO);

        // 刷新 Redis key 的有效期，实现滑动过期：
        // 用户每次携带有效 Token 请求，Token 的有效期都会重新变为 30 分钟。
        // 如果用户连续 30 分钟没有访问接口，Redis key 过期，Token 也就失效。
        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 本拦截器只负责识别用户和刷新有效期，不负责判断接口是否必须登录。
        // 因此无论是否查询到用户，这里都允许请求继续交给后续拦截器处理。
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 请求处理完成后清理 ThreadLocal 中的用户信息。
        // Tomcat 会复用工作线程，如果不清理，可能导致后续请求错误地使用上一个用户的信息。
        UserHolder.removeUser();
    }
}
