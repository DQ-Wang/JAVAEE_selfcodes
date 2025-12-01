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
@Table(name="goods_onsale")
@DynamicInsert
@DynamicUpdate
@EntityListeners(AuditingEntityListener.class)
public class OnSalePo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long shopId;

    private Long productId;

    private Long price;

    private LocalDateTime beginTime;

    private LocalDateTime endTime;

    private Integer quantity;

    private Byte type;

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


    private Integer maxQuantity;

    private Byte invalid;

}