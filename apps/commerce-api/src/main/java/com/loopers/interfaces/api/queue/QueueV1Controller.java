package com.loopers.interfaces.api.queue;

import com.loopers.application.queue.QueueService;
import com.loopers.domain.queue.QueueEntryResult;
import com.loopers.domain.queue.QueuePositionResult;
import com.loopers.domain.user.User;
import com.loopers.interfaces.api.ApiResponse;
import com.loopers.interfaces.api.LoginUser;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/queue")
public class QueueV1Controller {

    private final QueueService queueService;

    @PostMapping("/enter")
    public ApiResponse<QueueV1Dto.EnterResponse> enter(@LoginUser User user) {
        QueueEntryResult result = queueService.enter(user.getId());
        return ApiResponse.success(QueueV1Dto.EnterResponse.from(result));
    }

    @GetMapping("/position")
    public ApiResponse<QueueV1Dto.PositionResponse> getPosition(@LoginUser User user) {
        QueuePositionResult result = queueService.getPosition(user.getId());
        return ApiResponse.success(QueueV1Dto.PositionResponse.from(result));
    }
}
