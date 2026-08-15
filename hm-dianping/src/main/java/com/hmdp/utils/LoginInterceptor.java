package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.sf.jsqlparser.util.validation.metadata.NamedObject.user;

public class LoginInterceptor implements HandlerInterceptor {

//    private final StringRedisTemplate stringRedisTemplate;
//
//    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
//        this.stringRedisTemplate = stringRedisTemplate;
//    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        //获取session
////        HttpSession session = request.getSession();
//
//        //获取请求头中的token
//        String token = request.getHeader("authorization");
//        if (StrUtil.isBlank(token)) {
////            不存在拦截返回401
//            response.setStatus(401);
//            return false;
//        }
//        //获取session中的用户
////        Object user = session.getAttribute("user");
//
//        //基于redis获取redis的用户
//        String key = RedisConstants.LOGIN_USER_KEY + token;
//        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);
//
//        //判断用户是否登录
//        if (userMap.isEmpty()) {
//            response.setStatus(401);
//            return false;
//        }
//
////        if (user==null) {
////            //未登录，拦截
////            response.setStatus(401);
////            return false;
////        }
////        将查询到的Hash数据转为UserDTO
//        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
//
//        //登录了，保存用户 保存用户信息到 ThreadLocal
//        UserHolder.saveUser(userDTO);
//
////        刷新token有效期
//        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
//
//        //登录了放行

        if (UserHolder.getUser()==null){
            response.setStatus(401);
            return false;
        }
        return true;
    }

}
