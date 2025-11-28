//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis.dao;

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

    private static final String ONSALE_KEY_TEMPLATE = "product:onsale:%d";
    private static final String PRODUCT_ONSALE_REL_KEY_TEMPLATE = "product:onsale:list:%d";
    private static final long ONSALE_CACHE_TIMEOUT = 300;
    private static final long PRODUCT_ONSALE_REL_TIMEOUT = 300;

    /**
     * 获得货品的最近的价格和库存
     * @param productId 货品对象
     * @return 规格对象
     */
    public List<OnSale> getLatestOnSale(Long productId) throws DataAccessException {
        List<Long> cachedIds = getCachedRelation(productId);
        List<OnSale> cached = getCachedOnSales(cachedIds);
        if (cached != null) {
            log.debug("getLatestOnSale: hit cache for productId = {}", productId);
            return cached;
        }
        List<OnSale> latest = loadLatestOnSaleFromDb(productId);
        cacheRelation(productId, latest);
        return latest;
    }

    public void evictProductOnSaleCache(Long productId) {
        redisUtil.del(buildProductOnSaleKey(productId));
    }

    private List<OnSale> loadLatestOnSaleFromDb(Long productId) {
        LocalDateTime now = LocalDateTime.now();
        Pageable pageable = PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "endTime"));
        List<OnSalePo> onsalePoList = onSalePoMapper.findByProductIdEqualsAndBeginTimeBeforeAndEndTimeAfter(productId, now, now, pageable);
        return onsalePoList.stream().map(po -> CloneFactory.copy(new OnSale(), po)).collect(Collectors.toList());
    }

    private void cacheRelation(Long productId, List<OnSale> onSales) {
        List<Long> ids = onSales.stream().map(OnSale::getId).filter(id -> id != null).collect(Collectors.toList());
        redisUtil.set(buildProductOnSaleKey(productId), (Serializable) new ArrayList<>(ids), PRODUCT_ONSALE_REL_TIMEOUT);
        onSales.forEach(onSale -> redisUtil.set(buildOnSaleKey(onSale.getId()), onSale, ONSALE_CACHE_TIMEOUT));
    }

    @SuppressWarnings("unchecked")
    private List<Long> getCachedRelation(Long productId) {
        Object cache = redisUtil.get(buildProductOnSaleKey(productId));
        if (cache == null) {
            return null;
        }
        return (List<Long>) cache;
    }

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
