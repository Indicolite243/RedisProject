package com.hmdp.config;


import com.hmdp.utils.LoginInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Spring MVC 配置类。
 *
 * <p>这里集中注册 Web 层需要使用的拦截器、参数解析器等 MVC 组件。</p>
 */
@Configuration
public class MvcConfig implements WebMvcConfigurer {

    /**
     * 注册登录拦截器。
     *
     * <p>LoginInterceptor 默认拦截所有请求，只有登录、发送验证码以及
     * 部分公开查询接口可以在未登录状态下访问。</p>
     *
     * @param registry Spring MVC 的拦截器注册中心
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 添加登录拦截器。未登录请求会返回 401，已登录用户会被保存到 UserHolder。
        registry.addInterceptor(new LoginInterceptor())
                // 排除不需要登录即可访问的公开接口。
                .excludePathPatterns(
                        "/user/code",       // 发送手机验证码
                        "/user/login",      // 用户登录
                        "/blog/hot",        // 查询热门博客
                        "/shop/**",         // 商铺查询
                        "/shop-type/**",    // 商铺类型查询
                        "/upload/**",       // 博客图片上传
                        "/voucher/**"       // 优惠券查询和创建
                );
    }
}
