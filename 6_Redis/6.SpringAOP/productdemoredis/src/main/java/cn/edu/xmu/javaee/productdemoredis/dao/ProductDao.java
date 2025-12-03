//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis.dao;


import cn.edu.xmu.javaee.core.bean.RequestVariables;
import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.infrastructure.RedisUtil;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.UserToken;
import cn.edu.xmu.javaee.core.util.JacksonUtil;
import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.productdemoredis.dao.bo.OnSale;
import cn.edu.xmu.javaee.productdemoredis.dao.bo.Product;
import cn.edu.xmu.javaee.productdemoredis.mapper.GoodsPoMapper;
import cn.edu.xmu.javaee.productdemoredis.mapper.ProductPoMapper;
import cn.edu.xmu.javaee.productdemoredis.mapper.po.GoodsPo;
import cn.edu.xmu.javaee.productdemoredis.mapper.po.ProductPo;

import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import static cn.edu.xmu.javaee.core.model.Constants.PLATFORM;
/**
 * @author Ming Qiu
 **/
@Repository
@Slf4j
@RequiredArgsConstructor
public class ProductDao{

    private final ProductPoMapper productPoMapper;
    private final OnSaleDao onSaleDao;
    private final GoodsPoMapper goodsPoMapper;
    private final RequestVariables requestVariables;
    private final RedisUtil redisUtil;

    private static final String PRODUCT_KEY_TEMPLATE = "product:obj:%d";
    private static final String PRODUCT_RELATION_KEY_TEMPLATE = "product:relation:%d";
    private static final long PRODUCT_CACHE_TIMEOUT = 600;
    private static final long PRODUCT_RELATION_TIMEOUT = 300;

    /**
     * 用名称寻找Product对象
     *
     * @param name 名称
     * @return Product对象列表，带关联的Product返回
     */
    /**
     * 仅查“商品基本信息”，不加载 onSale / otherProduct。
     * 用于后台/列表场景，先查 DB 同时写入缓存，方便下次直接命中缓存。
     */
    public List<Product> retrieveSimpleProductByName(Long shopId, String name) throws BusinessException {
        List<Product> productList = new ArrayList<>();
        List<ProductPo> productPoList;
        Pageable pageable = PageRequest.of(1, 100);
        if (PLATFORM.equals(shopId)){
            productPoList = this.productPoMapper.findByName(name, pageable);
        } else {
            productPoList = this.productPoMapper.findByShopIdAndName(shopId, name, pageable);
        }
        for (ProductPo po : productPoList) {
            Product product = CloneFactory.copy(new Product(), po);
            cacheProduct(product);
            productList.add(product);
        }
        log.debug("retrieveSimpleProductByName: productList = {}", productList);
        return productList;
    }

    /**
     * 用id对象找Product对象
     *
     * @param shopId 商铺id
     * @param productId 产品id
     * @return Product对象，不关联的Product
     */
    /**
     * 查询“单个商品基本信息”，优先从缓存读取；未命中时落库加载，并写入缓存。
     */
    public Product findSimpleProductById(Long shopId, Long productId) throws BusinessException {
        Product product = this.getProductSnapshot(shopId, productId);
        log.debug("findSimpleProductById: product = {}", product);
        return product;
    }

    /**
     * 创建Product对象
     *
     * @param product 传入的Product对象
     * @return 返回对象ReturnObj
     */
    public Product insert(Product product) throws BusinessException {

        UserToken userToken = this.requestVariables.getUser();
        product.setCreatorId(userToken.getId());
        product.setCreatorName(userToken.getName());
        ProductPo po = CloneFactory.copy(new ProductPo(), product);
        log.debug("insert: po = {}", po);
        ProductPo ret = this.productPoMapper.save(po);
        Product newProduct = CloneFactory.copy(new Product(), ret);
        cacheProduct(newProduct);
        return newProduct;
    }

    /**
     * 修改商品信息
     *
     * @param product 传入的product对象
     * @return void
     */
    public void update(Product product) throws BusinessException {
        UserToken userToken = this.requestVariables.getUser();
        product.setModifierId(userToken.getId());
        product.setModifierName(userToken.getName());
        log.debug("update:  product = {}",  product);
        ProductPo oldPo = this.findPoById(userToken.getDepartId(), product.getId());
        log.debug("update: oldPo = {}", oldPo);
        ProductPo newPo = CloneFactory.copyNotNull(oldPo, product);
        log.debug("update: newPo = {}", newPo);
        this.productPoMapper.save(newPo);
        evictProductCache(product.getId());
    }

    /**
     * 删除商品
     *
     * @param id 商品id
     * @return
     */
    public void delete(Long id) throws BusinessException {
        UserToken userToken = this.requestVariables.getUser();
        this.findPoById(userToken.getDepartId(), id);
        this.productPoMapper.deleteById(id);
        evictProductCache(id);
    }

    /**
     * 分开的Entity对象
     * @param shopId 商铺id
     * @param productId 产品id
     * @return
     * @throws BusinessException
     */
    /**
     * 查询“商品全量信息”（含最新 OnSale、关联商品）。
     * 先读取基础信息（优先缓存），再补全关联数据。
     */
    public Product findById(Long shopId, Long productId) throws BusinessException {
        Product baseProduct = this.getProductSnapshot(shopId, productId);
        Product product = this.getFullProduct(baseProduct);
        log.debug("findById: product = {}", product);
        return product;
    }

    /**
     *
     * @param shopId 商铺id 为PLATFROM则在全系统寻找，否则在商铺内寻找
     * @param name 名称
     * @return Product对象列表，带关联的Product返回
     */
    /**
     * 查询“商品全量信息列表”，用于前台搜索。
     * 逻辑：先查 DB → 缓存基础信息 → 填充 onSale / 关联商品。
     */
    public List<Product> retrieveByName(Long shopId, String name) throws BusinessException {
        List<Product> productList = new ArrayList<>();
        Pageable pageable = PageRequest.of(0, 100);
        List<ProductPo> productPoList;
        if (PLATFORM.equals(shopId)) {
            productPoList = this.productPoMapper.findByName(name, pageable);
        }else{
            productPoList = this.productPoMapper.findByShopIdAndName(shopId, name, pageable);
        }
        for (ProductPo po : productPoList) {
            Product baseProduct = CloneFactory.copy(new Product(), po);
            cacheProduct(baseProduct);
            Product product = this.getFullProduct(baseProduct);
            productList.add(product);
        }
        log.debug("retrieveByName: productList = {}", productList);
        return productList;
    }

    /**
     * 获得关联的对象
     * @param productPo product po对象
     * @return 关联的Product对象
     * @throws DataAccessException
     */
    /**
     * 在已有基础信息的前提下，补齐 onSale & otherProduct，构成“完整商品”。
     * 注意：会深拷贝基础数据，避免缓存对象被篡改。
     */
    private Product getFullProduct(@NotNull Product baseProduct) throws DataAccessException {
        Product product = deepCopyProduct(baseProduct);
        log.debug("getFullProduct: product = {}",product);
        List<OnSale> latestOnSale = this.onSaleDao.getLatestOnSale(baseProduct.getId());
        product.setOnSaleList(latestOnSale);

        List<Product> otherProduct = this.retrieveOtherProduct(baseProduct.getId());
        product.setOtherProduct(otherProduct);
        log.debug("getFullProduct: fullproduct = {}",product);
        return product;
    }

    /**
     * 获得相关的产品对象
     * @param productPo product po对象
     * @return 相关产品对象列表
     * @throws DataAccessException
     */
    /**
     * 查询与当前商品相关的其他商品：
     *  1. 优先命中缓存（商品 -> 关联商品 ID 列表）；
     *  2. 未命中则通过 Goods 表查出关联关系，再批量查 ProductPo；
     *  3. 新结果写回缓存。
     */
    private List<Product> retrieveOtherProduct(Long productId) throws DataAccessException {
        List<Long> cachedRelationIds = getCachedRelationIds(productId);
        List<Product> cachedProducts = buildProductsFromCache(cachedRelationIds);
        if (cachedProducts != null) {
            log.debug("retrieveOtherProduct: hit cache for productId = {}", productId);
            return cachedProducts;
        }
        List<GoodsPo> goodsPos = this.goodsPoMapper.findByProductId(productId);
        if (goodsPos.isEmpty()) {
            cacheRelation(productId, Collections.emptyList());
            return Collections.emptyList();
        }
        List<Long> productIds = goodsPos.stream().map(GoodsPo::getRelateProductId).collect(Collectors.toList());
        List<ProductPo> productPoList = this.productPoMapper.findByIdIn(productIds);
        List<Product> relatedProducts = productPoList.stream().map(po -> {
            Product related = CloneFactory.copy(new Product(), po);
            cacheProduct(related);
            return related;
        }).collect(Collectors.toList());
        cacheRelation(productId, relatedProducts);
        return relatedProducts;
    }

    /**
     * 找到po对象，判断对象是否存在以及是否属于本商铺
     * @param shopId 商铺id
     * @param productId 商品id
     * @return RESOURCE_ID_OUTSCOPE, RESOURCE_ID_NOTEXIST
     */

    private ProductPo findPoById(Long shopId, Long productId){
        ProductPo productPo = this.productPoMapper.findById(productId).orElseThrow(() ->
                new BusinessException(ReturnNo.RESOURCE_ID_NOTEXIST, JacksonUtil.toJson(new String[] {"${product}", productId.toString()})));
        log.debug("findPoById: shopId = {}, productPo = {}", shopId, productPo);
        if (!Objects.equals(shopId, productPo.getShopId()) && !PLATFORM.equals(shopId)){
            String[] objects = new String[] {"${product}", productId.toString(), shopId.toString()};
            throw new BusinessException(ReturnNo.RESOURCE_ID_OUTSCOPE, JacksonUtil.toJson(objects));
        }
        return productPo;
    }

    /**
     * 查询“商品基础信息”，优先从缓存读取；未命中时落库，并缓存一份快照。
     */
    private Product getProductSnapshot(Long shopId, Long productId){
        Product cached = getCachedProduct(productId);
        if (cached != null){
            validateScope(shopId, cached.getShopId(), productId);
            return cached;
        }
        ProductPo productPo = this.findPoById(shopId, productId);
        Product product = CloneFactory.copy(new Product(), productPo);
        cacheProduct(product);
        return product;
    }

    /**
     * 缓存商品基础信息（不包含关联对象）。
     * 参考最佳实践：直接使用 CloneFactory 复制对象，避免手动 Builder 的繁琐。
     * 注意：不缓存 onSaleList 和 otherProduct，避免缓存过大和一致性问题。
     */
    private void cacheProduct(Product product){
        if (product == null || product.getId() == null){
            return;
        }
        // 使用 builder 复制对象字段（浅拷贝）
        Product snapshot = Product.builder()
                .id(product.getId())
                .shopId(product.getShopId())
                .name(product.getName())
                .originalPrice(product.getOriginalPrice())
                .weight(product.getWeight())
                .barcode(product.getBarcode())
                .unit(product.getUnit())
                .originPlace(product.getOriginPlace())
                .commissionRatio(product.getCommissionRatio())
                .freeThreshold(product.getFreeThreshold())
                .status(product.getStatus())
                .creatorId(product.getCreatorId())
                .creatorName(product.getCreatorName())
                .modifierId(product.getModifierId())
                .modifierName(product.getModifierName())
                .gmtCreate(product.getGmtCreate())
                .gmtModified(product.getGmtModified())
                .build();
        // 确保不缓存关联对象，避免缓存过大和一致性问题
        snapshot.setOnSaleList(null);
        snapshot.setOtherProduct(null);
        redisUtil.set(buildProductKey(product.getId()), snapshot, PRODUCT_CACHE_TIMEOUT);
    }

    private Product deepCopyProduct(Product source) {
        if (source == null) {
            return null;
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(source);
            return mapper.readValue(json, Product.class);
        } catch (Exception e) {
            log.error("Failed to deep copy Product: {}", e.getMessage());
            // Fallback: create new Product and copy fields manually
            return Product.builder()
                    .id(source.getId())
                    .shopId(source.getShopId())
                    .name(source.getName())
                    .originalPrice(source.getOriginalPrice())
                    .weight(source.getWeight())
                    .barcode(source.getBarcode())
                    .unit(source.getUnit())
                    .originPlace(source.getOriginPlace())
                    .commissionRatio(source.getCommissionRatio())
                    .freeThreshold(source.getFreeThreshold())
                    .status(source.getStatus())
                    .creatorId(source.getCreatorId())
                    .creatorName(source.getCreatorName())
                    .modifierId(source.getModifierId())
                    .modifierName(source.getModifierName())
                    .gmtCreate(source.getGmtCreate())
                    .gmtModified(source.getGmtModified())
                    .build();
        }
    }

    /**
     * 封装从 Redis 读取商品快照的逻辑。
     */
    private Product getCachedProduct(Long productId){
        return (Product) redisUtil.get(buildProductKey(productId));
    }

    private void evictProductCache(Long productId){
        redisUtil.del(buildProductKey(productId));
        redisUtil.del(buildProductRelationKey(productId));
        this.onSaleDao.evictProductOnSaleCache(productId);
    }

    private void validateScope(Long shopId, Long ownerShopId, Long productId){
        if (!Objects.equals(shopId, ownerShopId) && !PLATFORM.equals(shopId)){
            String[] objects = new String[] {"${product}", productId.toString(), shopId.toString()};
            throw new BusinessException(ReturnNo.RESOURCE_ID_OUTSCOPE, JacksonUtil.toJson(objects));
        }
    }

    @SuppressWarnings("unchecked")
    /**
     * 读取“商品 -> 关联商品 ID 列表”缓存；null 代表缓存缺失。
     */
    private List<Long> getCachedRelationIds(Long productId){
        Object cache = redisUtil.get(buildProductRelationKey(productId));
        if (cache == null){
            return null;
        }
        return (List<Long>) cache;
    }

    private void cacheRelation(Long productId, List<Product> relatedProducts){
        List<Long> relationIds = relatedProducts.stream().map(Product::getId).filter(Objects::nonNull).collect(Collectors.toList());
        redisUtil.set(buildProductRelationKey(productId), (Serializable) new ArrayList<>(relationIds), PRODUCT_RELATION_TIMEOUT);
    }

    /**
     * 根据 ID 列表构造关联商品列表：全部命中缓存才返回；任一缺失则返回 null。
     */
    private List<Product> buildProductsFromCache(List<Long> ids){
        if (ids == null){
            return null;
        }
        if (ids.isEmpty()){
            return Collections.emptyList();
        }
        List<Product> products = new ArrayList<>();
        for (Long id : ids){
            Product cached = getCachedProduct(id);
            if (cached == null){
                return null;
            }
            products.add(deepCopyProduct(cached));
        }
        return products;
    }

    private String buildProductKey(Long productId){
        return String.format(PRODUCT_KEY_TEMPLATE, productId);
    }

    private String buildProductRelationKey(Long productId){
        return String.format(PRODUCT_RELATION_KEY_TEMPLATE, productId);
    }
}
