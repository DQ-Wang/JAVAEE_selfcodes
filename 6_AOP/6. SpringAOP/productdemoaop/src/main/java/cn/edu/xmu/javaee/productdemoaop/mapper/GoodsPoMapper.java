package cn.edu.xmu.javaee.productdemoaop.mapper;

import cn.edu.xmu.javaee.productdemoaop.mapper.po.GoodsPo;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface GoodsPoMapper extends JpaRepository<GoodsPo, Long> {
    List<GoodsPo> findByProductId(Long productId);

}
