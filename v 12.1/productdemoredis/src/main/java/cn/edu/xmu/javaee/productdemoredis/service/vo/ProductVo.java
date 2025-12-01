//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis.service.vo;

import cn.edu.xmu.javaee.core.clonefactory.CopyFrom;
import cn.edu.xmu.javaee.productdemoredis.dao.bo.Product;
import cn.edu.xmu.javaee.productdemoredis.service.vo.CustomerProductVo;
import cn.edu.xmu.javaee.productdemoredis.service.vo.OnSaleVo;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.LocalDateTime;
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
@CopyFrom(Product.class)
public class ProductVo {

    private Long id;

    private Long shopId;

    private String skuSn;

    private String name;

    private Long originalPrice;

    private Long weight;

    private String barcode;

    private String unit;

    private String originPlace;

    @CopyFrom.Exclude(Product.class)
    private List<CustomerProductVo> otherProduct;

    @CopyFrom.Exclude(Product.class)
    private List<OnSaleVo> onsaleList;

    private String creator;

    private String modifier;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;

}
