package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadPoolExecutor

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

    fun recordOutgoing(accountName: String) {
        countersByAccount.getValue(accountName).outgoing.increment()
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