package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentMetrics: PaymentMetrics

    // At 200 requests/s and a 3-second deadline, at most 600 payments can still be useful.
    // Keep spare workers for event creation while provider calls occupy the allowed slots.
    private val maxWorkerCount = (
        paymentAccounts.sumOf { it.parallelRequests().toLong() } + 32L
    ).coerceIn(64L, 632L).toInt()

    private val paymentExecutor = ThreadPoolExecutor(
        64,
        maxWorkerCount,
        30L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(64),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler(Duration.ofMillis(250))
    )

    @PostConstruct
    fun registerMetrics() {
        paymentMetrics.monitorQueue(paymentExecutor)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        paymentMetrics.recordIncoming()
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}
