package com.loopers.interfaces.api.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.queue.EntryTokenRepository;
import com.loopers.domain.user.User;
import com.loopers.interfaces.api.ApiResponse;
import com.loopers.interfaces.api.UserAuthFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
@RequiredArgsConstructor
public class QueueTokenInterceptor implements HandlerInterceptor {

    private static final String HEADER_QUEUE_TOKEN = "X-Queue-Token";

    private final EntryTokenRepository entryTokenRepository;
    private final ObjectMapper objectMapper;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (!HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }

        User user = (User) request.getAttribute(UserAuthFilter.AUTHENTICATED_USER_ATTR);
        String requestToken = request.getHeader(HEADER_QUEUE_TOKEN);

        boolean valid = entryTokenRepository.find(user.getId())
                .map(storedToken -> storedToken.equals(requestToken))
                .orElse(false);

        if (!valid) {
            response.setStatus(HttpStatus.FORBIDDEN.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(objectMapper.writeValueAsString(
                    ApiResponse.fail("Forbidden", "유효한 입장 토큰이 없습니다.")
            ));
            return false;
        }

        return true;
    }
}
