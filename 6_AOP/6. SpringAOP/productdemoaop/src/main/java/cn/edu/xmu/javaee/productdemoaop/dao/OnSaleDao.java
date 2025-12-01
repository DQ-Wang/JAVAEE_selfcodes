//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.productdemoaop.dao;

import cn.edu.xmu.javaee.core.util.CloneFactory;
import cn.edu.xmu.javaee.productdemoaop.dao.bo.OnSale;
import cn.edu.xmu.javaee.productdemoaop.mapper.OnSalePoMapper;
import cn.edu.xmu.javaee.productdemoaop.mapper.po.OnSalePo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class OnSaleDao {

    private final OnSalePoMapper onSalePoMapper;

    /**
     * 获得货品的最近的价格和库存
     * @param productId 货品对象
     * @return 规格对象
     */
    public List<OnSale> getLatestOnSale(Long productId) throws DataAccessException {
        LocalDateTime now = LocalDateTime.now();
        List<OnSalePo> onsalePoList;
        Pageable pageable = PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "endTime"));
        onsalePoList = onSalePoMapper.findByProductIdEqualsAndBeginTimeBeforeAndEndTimeAfter(productId, now,now, pageable);
        return onsalePoList.stream().map(po-> CloneFactory.copy(new OnSale(), po)).collect(Collectors.toList());
    }
}
