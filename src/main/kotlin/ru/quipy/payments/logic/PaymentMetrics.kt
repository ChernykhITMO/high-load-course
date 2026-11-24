package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ThreadPoolExecutor

enum class PaymentCallOutcome(val tag: String, val retryable: Boolean = false) {
    SUCCESS("success"),
    PROVIDER_TIMEOUT("provider_timeout", retryable = true),
    TEMPORARY_ERROR("temporary_error", retryable = true),
    CLIENT_TIMEOUT("client_timeout"),
    FAILED("failed"),
}

@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
    @Value("\${payment.service-name}")
    private val serviceName: String,
    @Value("#{'\${payment.accounts}'.split(',')}")
    accountNames: List<String>,
) {
    private data class AccountCounters(
        val outgoing: Counter,
        val retries: Map<PaymentCallOutcome, Counter>,
        val callDurations: Map<PaymentCallOutcome, Timer>,
        val completedSuccess: Counter,
        val completedFailed: Counter,
    )

    private val incoming = counter(
        "shop.payment.incoming",
        "Payment requests received by OrderPayer",
    )

    private val countersByAccount = accountNames.distinct().associateWith { accountName ->
        AccountCounters(
            outgoing = counter(
                "shop.payment.outgoing",
                "Payment provider call attempts started",
                "accountName", accountName,
            ),
            retries = PaymentCallOutcome.entries.filter { it.retryable }.associateWith { outcome ->
                counter(
                    "shop.payment.retries",
                    "Repeated calls started after a retryable payment provider response",
                    "accountName", accountName,
                    "reason", outcome.tag,
                )
            },
            callDurations = PaymentCallOutcome.entries.associateWith { outcome ->
                Timer.builder("shop.payment.provider.call.duration")
                    .description("Duration of an individual payment provider call")
                    .tags("service", serviceName, "accountName", accountName, "outcome", outcome.tag)
                    .publishPercentileHistogram()
                    .serviceLevelObjectives(
                        Duration.ofMillis(1100),
                        Duration.ofMillis(1200),
                        Duration.ofMillis(1250),
                        Duration.ofMillis(1400),
                    )
                    .register(registry)
            },
            completedSuccess = counter(
                "shop.payment.completed",
                "Recorded payment attempt results",
                "accountName", accountName,
                "outcome", "success",
            ),
            completedFailed = counter(
                "shop.payment.completed",
                "Recorded payment attempt results",
                "accountName", accountName,
                "outcome", "failed",
            ),
        )
    }

    fun recordIncoming() {
        incoming.increment()
    }

    fun recordOutgoing(accountName: String, retryAfter: PaymentCallOutcome?) {
        val counters = countersByAccount.getValue(accountName)
        counters.outgoing.increment()
        retryAfter?.let { counters.retries.getValue(it).increment() }
    }

    fun recordCallDuration(accountName: String, outcome: PaymentCallOutcome, duration: Duration) {
        countersByAccount.getValue(accountName).callDurations.getValue(outcome).record(duration)
    }

    fun recordCompleted(accountName: String, success: Boolean) {
        val counters = countersByAccount.getValue(accountName)
        val counter = if (success) {
            counters.completedSuccess
        } else {
            counters.completedFailed
        }
        counter.increment()
    }

    fun monitorQueue(executor: ThreadPoolExecutor) {
        Gauge.builder("shop.payment.queue.size", executor) { pool ->
            pool.queue.size.toDouble()
        }
            .description("Payment tasks waiting in the executor queue")
            .tag("service", serviceName)
            .register(registry)
    }

    private fun counter(
        name: String,
        description: String,
        vararg tags: String,
    ): Counter =
        Counter.builder(name)
            .description(description)
            .tag("service", serviceName)
            .tags(*tags)
            .register(registry)
}
