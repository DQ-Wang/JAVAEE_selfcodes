//School of Informatics Xiamen University, GPL-3.0 license
package cn.edu.xmu.javaee.core.aop;

import cn.edu.xmu.javaee.core.bean.RequestVariables;
import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import cn.edu.xmu.javaee.core.model.ReturnObject;
import cn.edu.xmu.javaee.core.model.UserToken;
import cn.edu.xmu.javaee.core.util.JacksonUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cn.edu.xmu.javaee.core.model.Constants.BEGIN_TIME;
import static cn.edu.xmu.javaee.core.model.Constants.END_TIME;
import static cn.edu.xmu.javaee.core.util.Common.getI18nMessage;

/**
 * 用于控制器方面的Aspect
 */
@Aspect
@Order(10)
@Slf4j
@RequiredArgsConstructor
public class ControllerAspect {
    @Value("${javaee.core.page-size.max:1000}")
    private int max_page_size;

    @Value("${javaee.core.page-size.default:10}")
    private int default_page_size;

    private final RequestVariables requestVariables;
    private final MessageSource messageSource;
    /**
     * 所有返回值为ReturnObject的Controller
     *
     * @param jp
     * @return
     * @throws Throwable
     */
    @Around("cn.edu.xmu.javaee.core.aop.CommonPointCuts.controllers()")
    public Object doAround(ProceedingJoinPoint jp) throws Throwable {
        ReturnObject retVal = null;

        MethodSignature ms = (MethodSignature) jp.getSignature();
        HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest();
        HttpServletResponse response = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getResponse();

        MessageSourceAccessor messageSourceAccessor = new MessageSourceAccessor(this.messageSource, LocaleContextHolder.getLocale());
        String Authorization = request.getHeader("Authorization");
        if (Objects.nonNull(Authorization) && !Authorization.isEmpty() && !Authorization.isBlank()) {
            UserToken user = JacksonUtil.toObj(request.getHeader("Authorization"), UserToken.class);
            this.requestVariables.setUser(user);
        }

        String[] paramNames = ms.getParameterNames();
        log.debug("doAround: method = {}, paramNames = {}", ms.getName(), paramNames);
        Object[] args = jp.getArgs();
        try {
            Object[] newArgs = checkPageTimeLimit(request, paramNames, args);
            retVal = (ReturnObject) jp.proceed(newArgs);
        } catch (BusinessException exception) {
            log.info("doAround: BusinessException， errno = {}", exception.getErrno());
            retVal = new ReturnObject(exception.getErrno(), this.getI18nMessage(exception,messageSourceAccessor));
        }

        ReturnNo code = retVal.getCode();
        log.debug("doAround: jp = {}, code = {}", jp.getSignature().getName(), code);
        changeHttpStatus(code, response);

        return retVal;
    }

    /**
     * 根据code，修改reponse的HTTP Status code
     *
     * @param code
     * @param response
     */
    private void changeHttpStatus(ReturnNo code, HttpServletResponse response) {
        switch (code) {
            case CREATED:
                //201:
                response.setStatus(HttpServletResponse.SC_CREATED);
                break;

            case RESOURCE_ID_NOTEXIST:
                // 404：资源不存在
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                break;

            case AUTH_INVALID_JWT:
            case AUTH_JWT_EXPIRED:
            case AUTH_NEED_LOGIN:
            case AUTH_ID_NOTEXIST:
            case AUTH_USER_FORBIDDEN:
            case AUTH_INVALID_ACCOUNT:
                // 401
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                break;

            case INTERNAL_SERVER_ERR:
                // 500：数据库或其他严重错误
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;

            case FIELD_NOTVALID:
            case IMG_FORMAT_ERROR:
            case IMG_SIZE_EXCEED:
            case PARAMETER_MISSED:
            case INCONSISTENT_DATA:
                // 400
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                break;

            case RESOURCE_ID_OUTSCOPE:
            case AUTH_NO_RIGHT:
                // 403
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                break;
            default:
                response.setStatus(HttpServletResponse.SC_OK);
        }
        response.setContentType("application/json;charset=UTF-8");
    }

    /**
     * 获得国际化的message
     * @param e 错误Exception
     * @return 国际化的信息
     */
    private String getI18nMessage(BusinessException e, MessageSourceAccessor messageSourceAccessor) {
        String message = e.getMessage();
        String errMsg = e.getErrno().getMessage();
        if (Objects.isNull(message)) {
            message = messageSourceAccessor.getMessage(errMsg);
        }else{
            if (StringUtils.startsWithIgnoreCase(message, "[") && StringUtils.endsWithIgnoreCase(message, "]")) {
                //Array
                Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");
                List<String> args = JacksonUtil.parseObjectList(message, String.class).stream().map(arg -> {
                    Matcher matcher = pattern.matcher(arg);
                    String parsedArg = arg;
                    if (matcher.find()) {
                        parsedArg =  messageSourceAccessor.getMessage(matcher.group(1));
                    }
                    return parsedArg;
                }).collect(Collectors.toList());
                message = messageSourceAccessor.getMessage(errMsg, args.toArray());
            }
        }
        return message;
    }
    /**
     * 设置默认的page = 1和pageSize = 10
     * 防止客户端发过来pagesize过大的请求
     *
     * @author maguoqi
     *
     * @param request
     * @param paramNames
     * @param args
     */
    private Object[] checkPageTimeLimit(HttpServletRequest request, String[] paramNames, Object[] args) {
        Integer page = 1, pageSize = default_page_size;
        LocalDateTime beginTime = BEGIN_TIME, endTime = END_TIME;

        if (request != null) {

            String pageString = request.getParameter("page");
            String pageSizeString = request.getParameter("pageSize");
            String beginTimeString = request.getParameter("beginTime");
            String endTimeString = request.getParameter("endTime");

            if (null != pageString && !pageString.isEmpty() && pageString.matches("\\d+")) {
                page = Integer.valueOf(pageString);
                if (page <= 0) {
                    page = 1;
                }
            }

            if (null != pageSizeString && !pageSizeString.isEmpty() && pageSizeString.matches("\\d+")) {
                pageSize = Integer.valueOf(pageSizeString);
                if (pageSize <= 0 || pageSize > max_page_size) {
                    pageSize = default_page_size;
                }
            }

            try {
                if (null != beginTimeString && null != endTimeString && !beginTimeString.isEmpty() && !endTimeString.isEmpty()) {
                    beginTime = LocalDateTime.parse(beginTimeString);
                    endTime = LocalDateTime.parse(endTimeString);
                    if (beginTime.isAfter(endTime)) {
                        beginTime = BEGIN_TIME;
                        endTime = END_TIME;
                    }
                }
            } catch (Exception e) {
                log.debug("Exception occurs in time checking: {}", e.getMessage());
            }
        }

        for (int i = 0; i < paramNames.length; i++) {
            log.debug("checkPageTimeLimit: paramNames[{}] = {}", i, paramNames[i]);
            if (paramNames[i].equals("page")) {
                log.debug("checkPageTimeLimit: set {} to {}",paramNames[i], page);
                args[i] = page;
                continue;
            }

            if (paramNames[i].equals("pageSize")) {
                log.debug("checkPageTimeLimit: set {} to {}",paramNames[i], pageSize);
                args[i] = pageSize;
                continue;
            }

            if (paramNames[i].equals("beginTime") && (args[i] == null)){
                log.debug("checkPageTimeLimit: set {} to {}",paramNames[i], BEGIN_TIME);
                args[i] = beginTime;
                continue;
            }

            if (paramNames[i].equals("endTime") && (args[i] == null)){
                log.debug("checkPageTimeLimit: set {} to {}",paramNames[i], END_TIME);
                args[i] = endTime;
            }
        }
        return args;
    }
}
