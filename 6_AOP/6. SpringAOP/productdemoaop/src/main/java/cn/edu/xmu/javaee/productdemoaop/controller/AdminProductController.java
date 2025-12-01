package cn.edu.xmu.javaee.productdemoaop.controller;

import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.ReturnObject;
import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.core.validation.NewGroup;
import cn.edu.xmu.javaee.core.validation.UpdateGroup;
import cn.edu.xmu.javaee.productdemoaop.controller.dto.ProductDto;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.Product;
import cn.edu.xmu.javaee.productdemoaop.service.ProductService;
import cn.edu.xmu.javaee.productdemoaop.service.vo.ProductVo;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Locale;


/**
 * 商品控制器
 * @author Ming Qiu
 */
@RestController /*Restful的Controller对象*/
@RequestMapping(value = "/shops/{shopId}", produces = "application/json;charset=UTF-8")
@Slf4j
@RequiredArgsConstructor
public class AdminProductController {

    private final ProductService productService;
    private final MessageSource messageSource;

    @PostMapping("/products")
    public ReturnObject createDraft(@PathVariable Long shopId,
                                    @RequestBody @Validated(value = NewGroup.class) ProductDto dto, HttpServletRequest request) {
        log.debug("createDraft: dto = {}",dto);
        Product product = CloneFactory.copy(new Product(), dto);
        product.setShopId(shopId);
        ProductVo retProduct = this.productService.createProduct(product);
        ReturnObject retObj  = new ReturnObject(ReturnNo.CREATED, messageSource.getMessage(ReturnNo.CREATED.getMessage(), null, LocaleContextHolder.getLocale()), retProduct);
        return  retObj;
    }

    @GetMapping("/products/{id}")
    public ReturnObject getProductById(@PathVariable Long shopId, @PathVariable("id") Long id,HttpServletRequest request) {
        ProductVo productVo = this.productService.retrieveSimpleProductByID(shopId, id);
        ReturnObject retObj  = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()), productVo);
        return  retObj;
    }



    @GetMapping("/products")
    public ReturnObject searchProductByName(@PathVariable Long shopId, @RequestParam String name, HttpServletRequest request) {
        List<ProductVo> data = this.productService.retrieveProductByName(shopId, name);
        ReturnObject retObj = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()), data);
        return  retObj;
    }

    @PutMapping("/products/{id}")
    public ReturnObject modiProduct(@PathVariable Long id, @RequestBody @Validated(UpdateGroup.class) ProductDto productDto, HttpServletRequest request){
        Product product = CloneFactory.copy(new Product(), productDto);;
        product.setId(id);
        this.productService.modifyProduct(product);
        ReturnObject retObj  = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()));
        return  retObj;

    }

    @DeleteMapping("/products/{id}")
    public ReturnObject delProduct(@PathVariable("id") Long id, HttpServletRequest request) {
        this.productService.deleteProduct(id);
        ReturnObject retObj = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()));
        return  retObj;
    }
}
