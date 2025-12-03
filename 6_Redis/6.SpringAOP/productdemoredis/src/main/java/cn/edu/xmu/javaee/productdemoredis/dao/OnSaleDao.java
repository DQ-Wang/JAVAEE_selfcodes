//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis.dao;

// 引入 RedisUtil 是为了在 DAO 层操作 Redis 缓存工具类，如存储和获取商品上下架（OnSale）相关缓存信息
import cn.edu.xmu.javaee.core.infrastructure.RedisUtil;
import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.productdemoredis.dao.bo.OnSale;
import cn.edu.xmu.javaee.productdemoredis.mapper.OnSalePoMapper;
import cn.edu.xmu.javaee.productdemoredis.mapper.po.OnSalePo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class OnSaleDao {

    private final OnSalePoMapper onSalePoMapper;
    private final RedisUtil redisUtil;

    /**
     * Redis 缓存键模板：
     *  - product:onsale:<onSaleId>        单个 OnSale 详情缓存
     *  - product:onsale:list:<productId>  记录某商品“有效上架活动”的 OnSale ID 列表
     * 这样一来，就能做到“列表命中 + 每条命中”后完全跳过数据库读取。
     */
    private static final String ONSALE_KEY_TEMPLATE = "product:onsale:%d";
    private static final String PRODUCT_ONSALE_REL_KEY_TEMPLATE = "product:onsale:list:%d";
    private static final long ONSALE_CACHE_TIMEOUT = 300;
    private static final long PRODUCT_ONSALE_REL_TIMEOUT = 300;

    /**
     * 获取商品当前有效的上架活动。
     * 查询流程：
     *  1. 首先从 Redis 读取：每个商品（product）可能对应多个上架活动（OnSale），一个商品可以有多个 OnSale 记录。这里的“OnSale-ID”指的是上架活动（OnSale）的主键ID。如果 Redis 缓存中已经有这个商品对应的所有上架活动ID列表，并且这些 ID 对应的每个上架活动详细数据都在缓存中，就可以直接返回缓存结果，无需查数据库。
     *  2. 缓存未命中时，落库查询，并把列表及每条 OnSale 缓存下来；
     *  3. 返回数据给调用方。
     */
    public List<OnSale> getLatestOnSale(Long productId) throws DataAccessException {
        // 先尝试命中 Redis：若 ID 列表与对应 OnSale 都在缓存中，可直接返回
        List<Long> cachedIds = getCachedRelation(productId);
        List<OnSale> cached = getCachedOnSales(cachedIds);
        if (cached != null) {
            log.debug("getLatestOnSale: hit cache for productId = {}", productId);
            return cached;
        }
        // 缓存未命中则回源查询数据库，再写入缓存
        List<OnSale> latest = loadLatestOnSaleFromDb(productId);
        cacheRelation(productId, latest);
        return latest;
    }

    /**
     * 商品发生更新/删除时，由 ProductDao 调用该方法，清理“商品 → OnSale 列表”缓存，
     * 以保证后续查询会重新落库、刷新缓存。
     */
    public void evictProductOnSaleCache(Long productId) {
        redisUtil.del(buildProductOnSaleKey(productId));
    }

    /**
     * 从数据库中查询商品当前时间段的 OnSale 记录。只在缓存未命中时调用。
     */
    private List<OnSale> loadLatestOnSaleFromDb(Long productId) {
        LocalDateTime now = LocalDateTime.now();
        Pageable pageable = PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "endTime"));
        List<OnSalePo> onsalePoList = onSalePoMapper.findByProductIdEqualsAndBeginTimeBeforeAndEndTimeAfter(productId, now, now, pageable);
        return onsalePoList.stream().map(po -> CloneFactory.copy(new OnSale(), po)).collect(Collectors.toList());
    }

    /**
     * 同时缓存：
     *  1. 该商品对应的 OnSale-ID 列表（product:onsale:list:<productId>）；
     *  2. 每条 OnSale 的详情（product:onsale:<onSaleId>）。
     * 缓存列表时只保留 ID，避免重复写入大量对象；每条数据独立缓存，方便其他地方复用。
     * 
     * 优化：根据 OnSale 的 endTime 动态计算过期时间，确保缓存不会超过 OnSale 的实际有效期。
     */
    private void cacheRelation(Long productId, List<OnSale> onSales) {
        // 缓存商品与 OnSale 的关系（ID 列表）
        List<Long> ids = onSales.stream().map(OnSale::getId).filter(id -> id != null).collect(Collectors.toList());
        redisUtil.set(buildProductOnSaleKey(productId), (Serializable) new ArrayList<>(ids), PRODUCT_ONSALE_REL_TIMEOUT);
        
        // 缓存每条 OnSale 的详情，使用动态过期时间
        onSales.forEach(onSale -> {
            long timeout = getDynamicTimeout(onSale.getEndTime());
            redisUtil.set(buildOnSaleKey(onSale.getId()), onSale, timeout);
        });
    }

    /**
     * 计算动态过期时间，应该不超过 OnSale 的 endTime。
     * 参考最佳实践：根据业务时间动态计算，避免缓存过期后 OnSale 仍有效的情况。
     * 
     * @param endTime OnSale 的结束时间
     * @return 过期时间（秒），不超过配置的默认过期时间，也不超过 endTime
     */
    private long getDynamicTimeout(LocalDateTime endTime) {
        if (endTime == null) {
            return ONSALE_CACHE_TIMEOUT;
        }
        long diff = Duration.between(LocalDateTime.now(), endTime).toSeconds();
        // 如果 OnSale 已经过期，使用最小过期时间（避免立即失效）
        if (diff <= 0) {
            return 60; // 至少缓存 60 秒
        }
        // 取配置的过期时间和实际剩余时间的较小值
        return Math.min(ONSALE_CACHE_TIMEOUT, diff);
    }

    @SuppressWarnings("unchecked")
    /**
     * 获取某商品的 OnSale-ID 列表缓存；若缓存不存在返回 null，以便上层逻辑决定是否回源。
     */
    private List<Long> getCachedRelation(Long productId) {
        Object cache = redisUtil.get(buildProductOnSaleKey(productId));
        if (cache == null) {
            return null;
        }
        return (List<Long>) cache;
    }

    /**
     * 根据 ID 列表批量读取 OnSale 缓存。
     * 任何一个 OnSale 未命中即视为整体缓存失效，返回 null 让上层重新落库。
     */
    private List<OnSale> getCachedOnSales(List<Long> ids) {
        if (ids == null) {
            return null;
        }
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        List<OnSale> result = new ArrayList<>();
        for (Long id : ids) {
            OnSale cached = (OnSale) redisUtil.get(buildOnSaleKey(id));
            if (cached == null) {
                return null;
            }
            result.add(cached);
        }
        return result;
    }

    private String buildOnSaleKey(Long onSaleId) {
        return String.format(ONSALE_KEY_TEMPLATE, onSaleId);
    }

    private String buildProductOnSaleKey(Long productId) {
        return String.format(PRODUCT_ONSALE_REL_KEY_TEMPLATE, productId);
    }
}
