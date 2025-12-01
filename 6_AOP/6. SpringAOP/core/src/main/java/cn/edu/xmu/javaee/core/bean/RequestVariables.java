package cn.edu.xmu.javaee.core.bean;

import cn.edu.xmu.javaee.core.model.UserToken;
import lombok.Data;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

import java.util.Locale;

@Component
@RequestScope(proxyMode = ScopedProxyMode.TARGET_CLASS)
@Data
public class RequestVariables {
    private Locale locale = Locale.CHINA;
    private UserToken user;
}
