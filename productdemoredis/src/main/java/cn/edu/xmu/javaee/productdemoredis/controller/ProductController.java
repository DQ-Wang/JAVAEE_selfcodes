package cn.edu.xmu.javaee.productdemoredis.controller;

import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.ReturnObject;
import cn.edu.xmu.javaee.productdemoredis.service.ProductService;
import cn.edu.xmu.javaee.productdemoredis.service.vo.CustomerProductVo;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Locale;

import static cn.edu.xmu.javaee.core.model.Constants.PLATFORM;


/**
 * 商品控制器
 * @author Ming Qiu
 */
@RestController /*Restful的Controller对象*/
@RequestMapping(value = "/products", produces = "application/json;charset=UTF-8")
@Slf4j
@RequiredArgsConstructor
public class ProductController {

    private final  ProductService productService;
    private final MessageSource messageSource;

    @GetMapping("{id}")
    public ReturnObject getProductById(@PathVariable("id") Long id, HttpServletRequest request, HttpServletResponse response) {
        log.debug("getProductById: id = {} " ,id);
        CustomerProductVo product = this.productService.retrieveProductByID(PLATFORM, id);
        ReturnObject retObj = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()), product);
        return  retObj;
    }

    @GetMapping("")
    public ReturnObject searchProductByName(@RequestParam String name, HttpServletRequest request, HttpServletResponse response) {
        List<CustomerProductVo> data = this.productService.retrieveCustomerProductByName(PLATFORM, name);
        ReturnObject retObj = new ReturnObject(ReturnNo.OK, this.messageSource.getMessage(ReturnNo.OK.getMessage(), null, LocaleContextHolder.getLocale()), data);
        return  retObj;
    }
}
