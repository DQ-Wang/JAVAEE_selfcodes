package cn.edu.xmu.javaee.productdemoaop.mapper.po;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name = "goods_goods", schema = "oomall_demo")
@EntityListeners(AuditingEntityListener.class)
public class GoodsPo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long productId;

    private Long relateProductId;

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

}