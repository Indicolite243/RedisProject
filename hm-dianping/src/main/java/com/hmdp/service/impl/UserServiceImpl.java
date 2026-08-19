package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //校验手机号 使用工具类里面的方法
        if (RegexUtils.isPhoneInvalid(phone)) {
            //不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }

       //生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到session
//        session.setAttribute("code",code);
        //保存到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);

       //发送验证码
        log.debug("发送短信验证码成功，验证码:"+code);

        //返回结果
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }
        //从session校验验证码
//        Object cacheCode = session.getAttribute("code");
//        String code = loginForm.getCode();

        //从redis校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();

        if (cacheCode==null||!cacheCode.equals(code)) {
            //不一致报错
            return Result.fail("验证码错误！");
        }

        //一致 根据手机号查询用户 select * from user where phone = ?
        User user = query().eq("phone", phone).one();

        //判断用户是否存在
        if (user==null) {
            //不存在创建新用户并保存
            user = createUsercWithPhone(phone);
        }


        //保存用户信息到session
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));

//        随机生成token作为令牌
        String token = UUID.randomUUID().toString();

//        讲User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((filedName,fieldValue)-> fieldValue.toString()));

        //保存到redis
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
        //设置过期时间
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);

        //token返回客户端
        return Result.ok(token);
    }

    // ==================== 签到功能：Redis Bitmap（当前使用） ====================
    // Key格式：sign:用户id:年月，例如sign:1:202608
    // Bitmap中的offset从0开始，因此每月1日对应offset 0，19日对应offset 18
    // 一个用户每个月使用一个独立的Bitmap保存签到记录
    @Override
    public Result sign() {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        // 2. 获取当前日期
        LocalDateTime now = LocalDateTime.now();

        // 3. 拼接当前月份的签到Key
        // format前面带有冒号，例如:202608，最终Key为sign:1:202608
        String format = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + format;

        // 4. 获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();

        // 5. 写入Redis Bitmap：SETBIT key offset 1
        // Redis的offset从0开始，所以今天对应的offset需要使用dayOfMonth - 1
        // 重复签到只是把同一个bit再次设置为1，不会产生重复数据
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    // ==================== 连续签到统计：从今天向前统计（当前使用） ====================
    // 思路：读取本月1日到今天的签到记录，再从代表今天的最低位开始逐位判断
    // 遇到1说明当天已签到，计数器加1；遇到第一个0说明连续签到中断，结束统计
    @Override
    public Result signCount() {
        // 1. 获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        // 2. 获取当前日期
        LocalDateTime now = LocalDateTime.now();

        // 3. 拼接当前月份的签到Key
        String format = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + format;

        // 4. 获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();

        // 5. 获取本月1日到今天的所有签到记录
        // BITFIELD key GET u{dayOfMonth} 0：
        // 从offset 0开始读取dayOfMonth个无符号bit，并把二进制结果转换成十进制Long返回
        // 例如今天是19日，就读取u19；返回结果放在List的第一个位置
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );

        // 6. 本月没有任何签到记录，连续签到天数为0
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }

        // 7. 从二进制数字的最低位开始，向前统计连续签到天数
        // BITFIELD读取后，最低位代表今天，右边第二位代表昨天，依次向前
        int count = 0;
        while (true) {
            // num与1进行与运算，只保留num的最后一个bit位
            if ((num & 1) == 0) {
                // 最后一位为0，说明这一天未签到，连续签到在这里中断
                break;
            } else {
                // 最后一位为1，说明这一天已签到，连续签到天数加1
                count++;
            }

            // 数字无符号右移一位，抛弃刚刚判断过的最后一个bit位
            // 下一次循环就可以继续判断前一天是否签到
            num >>>= 1;
        }

        // 8. 返回从今天开始向前连续签到的天数
        return Result.ok(count);
    }

    private User createUsercWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        //保存用户 用mabatisplus提供的方法
        save(user);
        return user;
    }
}
