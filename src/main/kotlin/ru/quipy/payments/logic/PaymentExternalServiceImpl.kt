package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentMetrics: PaymentMetrics,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val paymentTimeReserveMillis = requestAverageProcessingTime.toMillis() * 2L
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val semaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
    ) {
        if (rejectIfTooLate(paymentId, paymentStartedAt, deadline)) {
            return
        }

        val waitUntilMillis = deadline - paymentTimeReserveMillis

        val semaphoreWaitMillis = (waitUntilMillis - now()).coerceAtLeast(0L)

        if (!semaphore.tryAcquire(semaphoreWaitMillis, TimeUnit.MILLISECONDS)) {
            rejectIfTooLate(paymentId, paymentStartedAt, deadline)
            return
        }

        try {
            if (!rateLimiter.tickBlocking(waitUntilMillis)) {
                rejectIfTooLate(paymentId, paymentStartedAt, deadline)
                return
            }

            if (rejectIfTooLate(paymentId, paymentStartedAt, deadline)) {
                return
            }

            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()

            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentStartedAt),
                )
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                paymentMetrics.recordOutgoing(accountName)

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(
                            response.body?.string(),
                            ExternalSysResponse::class.java,
                        )
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${e.message}",
                            e,
                        )
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            false,
                            e.message,
                        )
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            body.result,
                            now(),
                            transactionId,
                            reason = body.message,
                        )
                    }

                    paymentMetrics.recordCompleted(
                        accountName,
                        success = body.result,
                    )
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e,
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                false,
                                now(),
                                transactionId,
                                reason = "Request timeout.",
                            )
                        }

                        paymentMetrics.recordCompleted(
                            accountName,
                            success = false,
                        )
                    }

                    else -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e,
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                false,
                                now(),
                                transactionId,
                                reason = e.message,
                            )
                        }

                        paymentMetrics.recordCompleted(
                            accountName,
                            success = false,
                        )
                    }
                }
            }
        } finally {
            semaphore.release()
        }
    }

    private fun rejectIfTooLate(
        paymentId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
    ): Boolean {
        val checkedAt = now()
        val remainingMillis = deadline - checkedAt

        if (remainingMillis > paymentTimeReserveMillis) {
            return false
        }

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = false,
                transactionId = transactionId,
                startedAt = checkedAt,
                spentInQueueDuration = Duration.ofMillis(
                    checkedAt - paymentStartedAt
                ),
            )
        }

        paymentESService.update(paymentId) {
            it.logProcessing(
                success = false,
                processedAt = now(),
                transactionId = transactionId,
                reason = "Not enough time before payment deadline",
            )
        }

        paymentMetrics.recordCompleted(accountName, success = false)

        return true
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
