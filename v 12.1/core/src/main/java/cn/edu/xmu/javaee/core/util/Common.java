package cn.edu.xmu.javaee.core.util;

import cn.edu.xmu.javaee.core.exception.BusinessException;
import cn.edu.xmu.javaee.core.model.ReturnNo;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.support.MessageSourceAccessor;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Common {

    /**
     * 根据code，修改reponse的HTTP Status code
     *
     * @param code
     * @param response
     */
    public static void changeHttpStatus(ReturnNo code, HttpServletResponse response) {
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
    public static String getI18nMessage(BusinessException e, MessageSourceAccessor messageSourceAccessor) {
        String message = e.getMessage();
        String errMsg = e.getErrno().getMessage();
        if (Objects.isNull(message)) {
            message = messageSourceAccessor.getMessage(errMsg);
        }else{
            Pattern arrayPattern = Pattern.compile("\\[(.*?)\\]");
            Matcher arrayMatcher = arrayPattern.matcher(message);
            if (arrayMatcher.find()) {
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
}
