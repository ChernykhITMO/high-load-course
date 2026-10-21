package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.apigateway.APIController.OrderStatus
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

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

    private val paymentExecutor = ThreadPoolExecutor(
        128,
        128,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    @PostConstruct
    fun registerMetrics() {
        paymentMetrics.monitorQueue(paymentExecutor)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        paymentMetrics.recordIncoming()
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
