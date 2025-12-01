//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoaop.service.vo;

import cn.edu.xmu.javaee.core.clonefactory.CopyFrom;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.OnSale;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.Product;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.util.List;

/**
 * 商品视图对象
 * @author Ming Qiu
 **/
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
@CopyFrom({Product.class, OnSale.class})
public class CustomerProductVo {

    @CopyFrom.Of(Product.class)
    private Long id;

    private Long shopId;

    private String skuSn;

    private String name;

    private Long originalPrice;

    private Long weight;

    private Long price;

    private String barcode;

    private String unit;

    private String originPlace;

    private Integer quantity;

    private Integer maxQuantity;

    @CopyFrom.Exclude(Product.class)
    private List<CustomerProductVo> otherProduct;
}
