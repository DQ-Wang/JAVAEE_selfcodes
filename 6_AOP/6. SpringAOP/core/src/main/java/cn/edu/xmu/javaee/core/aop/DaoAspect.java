//School of Informatics Xiamen University, GPL-3.0 license

package cn.edu.xmu.javaee.core.aop;

import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Slf4j
public class DaoAspect {

    @Around("cn.edu.xmu.javaee.core.aop.CommonPointCuts.daos()")
    public Object doAround(ProceedingJoinPoint jp) throws Throwable {
        Object obj = null;

        MethodSignature ms = (MethodSignature) jp.getSignature();
        Object target = jp.getTarget();

        try {
            obj = jp.proceed();
            log.debug("doAround: obj = {}, method = {}", target, ms.getName());
        } catch(BusinessException e){
            throw e;
        }
        catch (Exception exception) {
            log.error("doAround: obj = {}, method = {}, e = {}", target, ms.getName(), exception);
            throw new BusinessException(ReturnNo.INTERNAL_SERVER_ERR, exception.getMessage());
        }
        return obj;
    }

}
