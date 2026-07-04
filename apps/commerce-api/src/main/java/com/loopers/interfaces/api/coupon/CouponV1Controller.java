package com.loopers.interfaces.api.coupon;

import com.loopers.application.coupon.CouponInfo;
import com.loopers.application.coupon.CouponIssueFacade;
import com.loopers.application.coupon.CouponIssueEventService;
import com.loopers.application.coupon.CouponService;
import com.loopers.domain.user.User;
import com.loopers.interfaces.api.ApiResponse;
import com.loopers.interfaces.api.LoginUser;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1")
public class CouponV1Controller {

    private final CouponService couponService;
    private final CouponIssueFacade couponIssueFacade;
    private final CouponIssueEventService couponIssueEventService;

    @ResponseStatus(HttpStatus.ACCEPTED)
    @PostMapping("/coupons/{couponId}/issue")
    public ApiResponse<CouponV1Dto.IssueResponse> issueCoupon(
            @LoginUser User user,
            @PathVariable Long couponId
    ) {
        String eventId = couponIssueFacade.requestIssue(couponId, user.getId());
        return ApiResponse.success(CouponV1Dto.IssueResponse.from(eventId));
    }

    @GetMapping("/coupons/issue/{eventId}")
    public ApiResponse<CouponV1Dto.IssueStatusResponse> getIssueStatus(
            @LoginUser User user,
            @PathVariable String eventId
    ) {
        CouponInfo.IssueEvent info = couponIssueEventService.getIssueEvent(eventId);
        return ApiResponse.success(CouponV1Dto.IssueStatusResponse.from(info));
    }

    @GetMapping("/users/me/coupons")
    public ApiResponse<List<CouponV1Dto.MyCouponResponse>> getMyCoupons(
            @LoginUser User user
    ) {
        List<CouponInfo.MyCoupon> coupons = couponService.getUserCoupons(user.getId());
        return ApiResponse.success(coupons.stream().map(CouponV1Dto.MyCouponResponse::from).toList());
    }
}
