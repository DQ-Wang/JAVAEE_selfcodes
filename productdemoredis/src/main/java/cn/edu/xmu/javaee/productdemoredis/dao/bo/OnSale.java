//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoredis.dao.bo;

import cn.edu.xmu.javaee.core.clonefactory.CopyFrom;
import cn.edu.xmu.javaee.core.clonefactory.CopyNotNullTo;
import cn.edu.xmu.javaee.productdemoredis.mapper.po.OnSalePo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
@CopyFrom({OnSalePo.class})
@CopyNotNullTo({OnSalePo.class})
public class OnSale implements Serializable {

    private static final long serialVersionUID = 1L;
    private Long id;

    private Long price;

    private LocalDateTime beginTime;

    private LocalDateTime endTime;

    private Integer quantity;

    private Integer maxQuantity;

    private String skuSn;

    private Long creatorId;

    private String creatorName;

    private Long modifierId;

    private String modifierName;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;
}
