# Case 4: 200 payments per second with a 3-second deadline

Test account: `acc-18`. The test sends 6,000 payments at 200 per second, so arrivals last about 30 seconds. Each payment must finish within 3 seconds of its own arrival.

## Design

The payment worker pool grows from 64 threads up to the number of provider parallel slots plus 32, capped at 632. The cap is based on 200 arrivals/second times the 3-second useful lifetime (600), with 32 spare workers. A small queue causes the pool to grow under load instead of leaving time-sensitive payments behind thousands of queued tasks.

Each account still has its own semaphore and rolling one-second RPS limiter. The limiter now reserves a slot atomically with its timestamp, so concurrent requests cannot exceed the sliding-window limit. Waiting for either limit ends before the payment deadline, leaving time for the provider call. The HTTP call itself is also bounded by the remaining deadline. Payments that cannot be attempted in time are recorded as failures without paying for a provider request. Per-payment messages use debug logging to avoid excessive output at 200 requests/second.

## Capacity check

The account's actual SLA is fetched from the provider on startup. Its approximate sustainable throughput is:

`min(rateLimitPerSec, parallelRequests / averageProcessingTimeSeconds)`.

If this is below 200/second, the input rate exceeds the provider's capacity; no client-side scheduling algorithm can make every payment succeed within 3 seconds. The actual success rate also depends on processing-time variation, database latency, and the provider's response distribution, so it must be measured with the full test.

## Run

Use Java 17, set `PAYMENT_SERVICE_NAME` and `PAYMENT_TOKEN` in the process environment, and start the Compose services. This branch defaults `PAYMENT_ACCOUNTS` to `acc-18`. Start the shop with `mvn spring-boot:run`, then run `test-local-run.http`. For remote testing, `test-on-prem-run.http` points to `hw-04` and `acc-18`. Keep the token out of Git.
