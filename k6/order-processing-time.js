import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const BASE_URL = 'http://localhost:8080';

const orderDuration = new Trend('order_duration', true);
const errorRate = new Rate('error_rate');
const successCount = new Counter('success_count');

const PRODUCT_IDS = [
    45, 112, 144, 162, 261, 312, 391, 487, 655, 693,
    837, 923, 1228, 1233, 1354, 1538, 1639, 1737, 1796, 1996,
    2001, 2147, 2226, 2285, 2312, 2379, 2391, 2420, 2460, 2461,
    2583, 2953, 2987, 3059, 3161, 3214, 3222, 3280, 3499, 3569,
    3662, 3843, 3860, 3907, 3921, 3925, 4055, 4163, 4179, 4191,
];

export const options = {
    scenarios: {
        order_load: {
            executor: 'constant-vus',
            vus: 40,        // DB 커넥션 풀 수만큼
            duration: '30s',
        },
    },
    thresholds: {
        order_duration: ['p(50)<500', 'p(95)<2000'],
        error_rate: ['rate<0.05'],
    },
};

export default function () {
    const productId = PRODUCT_IDS[Math.floor(Math.random() * PRODUCT_IDS.length)];

    const res = http.post(
        `${BASE_URL}/api/v1/orders`,
        JSON.stringify({ items: [{ productId: productId, quantity: 1 }] }),
        {
            headers: {
                'Content-Type': 'application/json',
                'X-Loopers-LoginId': 'k6user',
                'X-Loopers-LoginPw': 'pAssWord1!',
            },
        }
    );

    const ok = check(res, {
        'status 201': (r) => r.status === 201,
    });

    orderDuration.add(res.timings.duration);
    errorRate.add(!ok);
    if (ok) successCount.add(1);
}
