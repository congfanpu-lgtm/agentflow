package com.agentflow.server.product;

import com.agentflow.server.entity.UserEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.UserMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 登录用户自己的数据。每个方法第一步都是 {@code sessions.require(req)} 取当前用户 id,
 * 之后所有查询都带这个 id —— 接口里没有任何"传一个用户 id 进来"的参数,所以看不到也改不了别人的数据。
 * 路径段只用小写字母和连字符:DemoGuard 靠这个形状识别产品接口(见其注释)。
 */
@RestController
@RequestMapping("/api/v1/me")
@RequiredArgsConstructor
public class MeController {

    private final Sessions sessions;
    private final UserMapper userMapper;
    private final WatchlistMapper watchlistMapper;

    @GetMapping
    public Map<String, Object> me(HttpServletRequest req) {
        UserEntity user = user(req);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("email", user.getEmail());
        out.put("pushHour", user.getPushHour());
        out.put("timezone", user.getTimezone());
        return out;
    }

    @GetMapping("/watchlist")
    public List<Map<String, String>> watchlist(HttpServletRequest req) {
        return watchlistOf(sessions.require(req)).stream()
                .map(w -> Map.of("ticker", w.getTicker(), "companyName", w.getCompanyName())).toList();
    }

    private List<WatchlistEntity> watchlistOf(long userId) {
        return watchlistMapper.selectList(new QueryWrapper<WatchlistEntity>().eq("user_id", userId).orderByAsc("id"));
    }

    private UserEntity user(HttpServletRequest req) {
        UserEntity user = userMapper.selectById(sessions.require(req));
        if (user == null) {   // 会话还在但账号没了
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "请先登录");
        }
        return user;
    }
}
