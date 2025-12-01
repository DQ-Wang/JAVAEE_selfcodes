package cn.edu.xmu.javaee.productdemoaop.mapper;

import cn.edu.xmu.javaee.productdemoaop.mapper.po.OnSalePo;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface OnSalePoMapper extends JpaRepository<OnSalePo, Long> {

    List<OnSalePo> findByProductIdEqualsAndBeginTimeBeforeAndEndTimeAfter(Long productId, LocalDateTime beginTime, LocalDateTime endTime, Pageable pageable);

}
