package cn.edu.xmu.javaee.productdemoredis.mapper;

import cn.edu.xmu.javaee.productdemoredis.mapper.po.ProductPo;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ProductPoMapper extends JpaRepository<ProductPo, Long> {
    List<ProductPo> findByName(String name, Pageable pageable);
    List<ProductPo> findByShopIdAndName(Long shopId, String name, Pageable pageable);
    List<ProductPo> findByIdIn(List<Long> ids);

}
