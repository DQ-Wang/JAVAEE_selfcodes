//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.core.aop;

import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.ReturnObject;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/**
 * 处理RestController的validation Exception
 */
@RestControllerAdvice
@Slf4j
public class ControllerAdvice {

    /**
     * 需要配置
     * server.servlet.encoding.charset=UTF-8
     * server.servlet.encoding.enabled=true
     * server.servlet.encoding.force=true
     * 否则中文返回是乱码
     * @param e
     * @return
     */
    @ExceptionHandler({BindException.class, ValidationException.class, MethodArgumentNotValidException.class})
    @ResponseStatus(code = HttpStatus.BAD_REQUEST)
    public ReturnObject bindExceptionHandler(Exception e){
        StringBuilder sb = new StringBuilder();

        if (e instanceof BindException) {
            List<FieldError> allErrors = ((BindException) e).getFieldErrors();
            for (FieldError error : allErrors) {
                sb.append(String.format("%s:%s,", error.getField(), error.getDefaultMessage()));
            }
        }else if (e instanceof  MethodArgumentNotValidException){
            BindingResult bindingResult = ((MethodArgumentNotValidException) e).getBindingResult();
            FieldError error = bindingResult.getFieldError();
            sb.append(error.getDefaultMessage());
        }else{
            sb.append(e.getMessage());
        }
        log.debug("bindExceptionHandler: e = {}",sb.toString());
        return new ReturnObject(ReturnNo.FIELD_NOTVALID, sb.toString());
    }

    @ResponseStatus(code = HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = NumberFormatException.class)
    public ReturnObject numberFormatException(NumberFormatException exception) {
        log.debug("numberFormatNotValid: msg = {}", exception.getMessage());
        return new ReturnObject(ReturnNo.FIELD_NOTVALID, exception.getMessage());
    }

    @ResponseStatus(code = HttpStatus.BAD_REQUEST)
    @ExceptionHandler(value = HttpMessageNotReadableException.class)
    public ReturnObject httpMessageNotReadableException(HttpMessageNotReadableException exception) {
        log.debug("httpMessageNotReadableException: msg = {}", exception.getMessage());
        return new ReturnObject(ReturnNo.FIELD_NOTVALID, exception.getMessage());
    }

}
