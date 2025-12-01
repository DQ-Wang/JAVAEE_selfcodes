package cn.edu.xmu.javaee.productdemoredis.dao.bo;

import cn.edu.xmu.javaee.core.clonefactory.CopyFrom;
import cn.edu.xmu.javaee.core.clonefactory.CopyNotNullTo;
import cn.edu.xmu.javaee.core.clonefactory.CopyTo;
import cn.edu.xmu.javaee.productdemoredis.mapper.po.ProductPo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 商品规格
 * @author Ming Qiu
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@CopyFrom({ProductPo.class})
@CopyNotNullTo({ProductPo.class})
@CopyTo({ProductPo.class})
public class Product implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 代理对象
     */
    private Long id;

    private Long shopId;

    private List<Product> otherProduct;

    private List<OnSale> onSaleList;

    private String name;

    private Long originalPrice;

    private Long weight;

    private String barcode;

    private String unit;

    private String originPlace;

    private Integer commissionRatio;

    private Long freeThreshold;

    private Byte status;

    private Long creatorId;

    private String creatorName;

    private Long modifierId;

    private String modifierName;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;
}
