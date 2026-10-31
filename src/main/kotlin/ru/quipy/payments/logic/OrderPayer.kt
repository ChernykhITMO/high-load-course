package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.min

@Service
class OrderPayer(
    paymentAccountProperties: List<PaymentAccountProperties>,
    @Value("\${payment.max-queue-wait-seconds:10}")
    maxQueueWaitSeconds: Double,
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

    @Autowired
    private lateinit var orderRepository: OrderRepository

    init {
        require(maxQueueWaitSeconds.isFinite() && maxQueueWaitSeconds > 0) {
            "Payment queue wait budget must be a positive number of seconds"
        }
        require(paymentAccountProperties.isNotEmpty()) { "No payment accounts configured" }
        paymentAccountProperties.forEach {
            require(it.rateLimitPerSec > 0 && it.parallelRequests > 0 && it.averageProcessingTime.toMillis() > 0) {
                "Invalid processing limits for payment account ${it.accountName}"
            }
        }
    }

    private val averageProcessingSeconds = paymentAccountProperties.sumOf {
        it.averageProcessingTime.toMillis() / 1000.0
    }
    private val effectiveRps = paymentAccountProperties.minOf {
        val accountProcessingSeconds = it.averageProcessingTime.toMillis() / 1000.0
        min(it.rateLimitPerSec.toDouble(), it.parallelRequests / accountProcessingSeconds)
    }
    private val workerCount = ceil(effectiveRps * averageProcessingSeconds * 1.2).toInt().coerceAtLeast(1)
    private val queueCapacity = floor(effectiveRps * maxQueueWaitSeconds).toInt().coerceAtLeast(1)

    private val incomingRateLimiter = TokenBucketRateLimiter(
        rate = paymentAccountProperties.minOf { it.rateLimitPerSec },
        bucketMaxCapacity = queueCapacity + paymentAccountProperties.minOf { it.rateLimitPerSec },
        window = 1,
        timeUnit = TimeUnit.SECONDS,
    )

    private val paymentExecutor = ThreadPoolExecutor(
        workerCount,
        workerCount,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    @PostConstruct
    fun registerMetrics() {
        paymentMetrics.monitorQueue(paymentExecutor)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        paymentMetrics.recordIncoming()
        if (!incomingRateLimiter.tick()) {
            throw RejectedExecutionException("Payment admission rate limit exceeded")
        }
        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            val order = orderRepository.findById(orderId)
                ?: throw IllegalArgumentException("No such order $orderId")
            orderRepository.save(order.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))

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
