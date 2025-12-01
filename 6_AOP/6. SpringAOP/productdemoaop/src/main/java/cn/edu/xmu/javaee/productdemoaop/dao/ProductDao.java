//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoaop.dao;


import cn.edu.xmu.javaee.core.bean.RequestVariables;
import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.UserToken;
import cn.edu.xmu.javaee.core.util.JacksonUtil;
import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.OnSale;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.Product;
import cn.edu.xmu.javaee.productdemoaop.mapper.GoodsPoMapper;
import cn.edu.xmu.javaee.productdemoaop.mapper.ProductPoMapper;
import cn.edu.xmu.javaee.productdemoaop.mapper.po.GoodsPo;
import cn.edu.xmu.javaee.productdemoaop.mapper.po.ProductPo;

import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

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

    /**
     * 用名称寻找Product对象
     *
     * @param name 名称
     * @return Product对象列表，带关联的Product返回
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
    public Product findSimpleProductById(Long shopId, Long productId) throws BusinessException {
        Product product = null;
        ProductPo productPo = this.findPoById(shopId,productId);
        product = CloneFactory.copy(new Product(), productPo);

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
        return CloneFactory.copy(new Product(), ret);
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
    }

    /**
     * 分开的Entity对象
     * @param shopId 商铺id
     * @param productId 产品id
     * @return
     * @throws BusinessException
     */
    public Product findById(Long shopId, Long productId) throws BusinessException {
        Product product = null;
        ProductPo productPo = this.findPoById(shopId,productId);
        product = this.getFullProduct(productPo);
        log.debug("findById: product = {}", product);
        return product;
    }

    /**
     *
     * @param shopId 商铺id 为PLATFROM则在全系统寻找，否则在商铺内寻找
     * @param name 名称
     * @return Product对象列表，带关联的Product返回
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
            Product product = this.getFullProduct(po);
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
    private Product getFullProduct(@NotNull ProductPo productPo) throws DataAccessException {
        Product product = CloneFactory.copy(new Product(), productPo);
        log.debug("getFullProduct: product = {}",product);
        List<OnSale> latestOnSale = this.onSaleDao.getLatestOnSale(productPo.getId());
        product.setOnSaleList(latestOnSale);

        List<Product> otherProduct = this.retrieveOtherProduct(productPo);
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
    private List<Product> retrieveOtherProduct(@NotNull ProductPo productPo) throws DataAccessException {
        List<ProductPo> productPoList;
        List<GoodsPo> goodsPos = this.goodsPoMapper.findByProductId(productPo.getId());
        List<Long> productIds = goodsPos.stream().map(GoodsPo::getRelateProductId).collect(Collectors.toList());
        productPoList = this.productPoMapper.findByIdIn(productIds);
        return productPoList.stream().map(po -> CloneFactory.copy(new Product(), po)).collect(Collectors.toList());
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
}
