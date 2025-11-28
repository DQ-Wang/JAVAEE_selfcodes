package cn.edu.xmu.javaee.productdemoredis.service;

import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.productdemoredis.dao.ProductDao;
import cn.edu.xmu.javaee.productdemoredis.dao.bo.Product;
import cn.edu.xmu.javaee.productdemoredis.service.vo.CustomerProductVo;
import cn.edu.xmu.javaee.productdemoredis.service.vo.ProductVo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class ProductService {

    private final ProductDao productDao;

    /**
     * 获取某个商品信息，仅展示相关内容
     *
     * @param id 商品id
     * @return 商品对象
     */
    public CustomerProductVo retrieveProductByID(Long shopId, Long id) throws BusinessException {
        assert shopId != null && id != null;
        log.debug("findProductById: shopId = {}, id = {}",shopId, id);
        Product product =  this.productDao.findById(shopId, id);
        CustomerProductVo customerProductVo = getCustomerProductVo(product);
        customerProductVo.setOtherProduct(getOtherProduct(product));
        return customerProductVo;
    }


    public ProductVo retrieveSimpleProductByID(Long shopId, Long id) throws BusinessException {
        assert shopId != null && id != null;
        log.debug("retrieveSimpleProductByID: shopId = {}, id = {}",shopId, id);
        Product product = this.productDao.findSimpleProductById(shopId, id);
        return CloneFactory.copy(new ProductVo(), product);
    }

    /**
     * 用商品名称搜索商品
     * @param shopId 商铺id
     * @param name 商品名称
     *
     * @return 商品对象
     */
    public List<CustomerProductVo> retrieveCustomerProductByName(Long shopId, String name) throws BusinessException{
        assert shopId != null && name != null;
        List<Product> productList =  this.productDao.retrieveByName(shopId, name);
        List<CustomerProductVo> data = productList.stream().map(o->{
            CustomerProductVo vo = getCustomerProductVo(o);
            vo.setOtherProduct(getOtherProduct(o));
            return vo;
        }).collect(Collectors.toList());
        return data;
    }

    /**
     * 用商品名称搜索商品
     * @param shopId 商铺id
     * @param name 商品名称
     *
     * @return 商品对象
     */
    public List<ProductVo> retrieveProductByName(Long shopId, String name) throws BusinessException{
        assert shopId != null && name != null;
        List<Product> productList =  this.productDao.retrieveByName(shopId, name);
        List<ProductVo> data = productList.stream().map(o->CloneFactory.copy(new ProductVo(), o)).collect(Collectors.toList());
        return data;
    }

    /**
     * 新增商品
     * @param product 新商品信息
     * @return 新商品
     */
    public ProductVo createProduct(Product product) throws BusinessException{
        assert product != null;
        Product retProduct = this.productDao.insert(product);
        Product newProduct = this.productDao.findById(retProduct.getShopId(),retProduct.getId());
        ProductVo vo = CloneFactory.copy(new ProductVo(), newProduct);
        return vo;
    }


    /**
     * 修改商品
     * @param product 修改商品信息
     */
    public void modifyProduct(Product product) throws BusinessException{
        assert product != null && product.getId() != null;
        this.productDao.update(product);
    }

    /** 删除商品
     * @param id 商品id
     * @return 删除是否成功
     */
    public void deleteProduct(Long id) throws BusinessException {
        assert id != null;
        this.productDao.delete(id);
    }

    /**
     * 从Product拷贝到CustomerProductVo
     * @param o product
     * @return CustomerProductVo
     */
    private CustomerProductVo getCustomerProductVo(Product o) {
        CustomerProductVo vo = CloneFactory.copy(new CustomerProductVo(), o);
        if (Objects.nonNull(o.getOnSaleList())){
            CloneFactory.copy(vo, o.getOnSaleList().get(0));
        }

        return vo;
    }

    private List<CustomerProductVo> getOtherProduct(Product product) {
        List<Product> otherProductList = product.getOtherProduct();
        List<CustomerProductVo> otherProduct = otherProductList.stream().map(product1 -> getCustomerProductVo(product1)).collect(Collectors.toUnmodifiableList());
        return otherProduct;
    }
}
