package cn.edu.xmu.javaee.core.bean;

import cn.edu.xmu.javaee.core.model.UserToken;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.AuditorAware;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
@RequiredArgsConstructor
public class UserAuditor implements AuditorAware<Long> {

    private final RequestVariables requestVariables;

    @Override
    public Optional<Long> getCurrentAuditor() {
        UserToken userToken = this.requestVariables.getUser();
        return Optional.of(userToken.getId());
    }
}
