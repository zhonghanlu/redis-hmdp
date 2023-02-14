package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryShopId(Long id) {
        //缓存穿透
//        Shop shop = pass(id);
        //缓存击穿 互斥锁
//        Shop shop = multNx(id);
        //缓存击穿 逻辑过期
        Shop shop = logicEx(id);
        return Result.ok(shop);
    }

    private Shop logicEx(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        Shop confirm = confirm(shopJson);
        if (Objects.nonNull(confirm)) {
            return confirm;
        }
        String lockKey = LOCK_SHOP_KEY + id;
        //已经过期进行键重建
        Boolean isLock = tryLock(lockKey);
        if (isLock) {
            //此操作之前進行再次尝试命中
            Shop confirm2 = confirm(stringRedisTemplate.opsForValue().get(key));
            if (Objects.nonNull(confirm2)) {
                return confirm2;
            }
            //获得锁进行 新启线程进行 重建锁
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    save2Redis(id, LOCK_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    delLock(lockKey);
                }
            });
        }
        return JSONUtil.toBean((JSONObject) JSONUtil.toBean(shopJson, RedisData.class).getData(), Shop.class);
    }

    public Shop confirm(String shopJson) {
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop bean = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //在此时间之前没过期直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return bean;
        }
        return null;
    }

    public void save2Redis(Long id, Long ex) {
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(ex));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    private Shop multNx(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        if ("".equals(shopJson)) {
            return null;
        }

        Shop shop = null;
        String lockKey = LOCK_SHOP_KEY + id;
        try {
            //获取锁
            Boolean isFlag = tryLock(lockKey);
            if (!isFlag) {
                return multNx(id);
            }
            //重建键之前再次进行缓存数据查询
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            //重建数据键
            shop = getById(id);
            if (Objects.isNull(shop)) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return shop;
            }
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            delLock(lockKey);
        }
        return shop;
    }

    /**
     * 缓存穿透
     */
    public Shop pass(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        if ("".equals(shopJson)) {
            return null;
        }

        Shop shop = getById(id);
        if (Objects.isNull(shop)) {
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    public Boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    public void delLock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result update(Shop shop) {
        if (Objects.isNull(shop.getId())) {
            return Result.fail("商品id不可为空");
        }
        update(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }
}
