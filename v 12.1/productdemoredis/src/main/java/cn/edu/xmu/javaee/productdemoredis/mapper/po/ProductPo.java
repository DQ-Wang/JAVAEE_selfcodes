package cn.edu.xmu.javaee.productdemoredis.mapper.po;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Table(name="goods_product")
@DynamicUpdate
@DynamicInsert
@EntityListeners(AuditingEntityListener.class)
public class ProductPo{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long shopId;

    private Long categoryId;

    private Long templateId;

    private String name;

    private Long originalPrice;

    private Long weight;

    private String barcode;

    private String unit;

    private String originPlace;

    @CreatedBy
    @Column(updatable = false)
    private String creator;

    @LastModifiedBy
    @Column(insertable = false)
    private String modifier;

    @CreatedDate
    @Column(insertable = false, updatable = false)
    private LocalDateTime gmtCreate;

    @LastModifiedDate
    @Column(insertable = false)
    private LocalDateTime gmtModified;

    private Byte status;

    private Integer commissionRatio;

    private Long shopLogisticId;

    private Long freeThreshold;
}