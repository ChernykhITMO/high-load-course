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
import java.io.IOException
import java.io.InterruptedIOException
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
        private const val RESPONSE_GRACE_MILLIS = 200L
        private const val ACC_9_OUTGOING_RATE_LIMIT_PER_SEC = 110
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val paymentTimeReserveMillis = requestAverageProcessingTime.toMillis()
    private val providerAttemptTimeoutMillis = paymentTimeReserveMillis.takeIf { accountName == "acc-7" }
    private val rateLimitPerSec = if (accountName == "acc-9") {
        minOf(properties.rateLimitPerSec, ACC_9_OUTGOING_RATE_LIMIT_PER_SEC)
    } else {
        properties.rateLimitPerSec
    }
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(providerAttemptTimeoutMillis == null)
        .build()
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

        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            var retryAfter: PaymentCallOutcome? = null
            repeat(3) { attempt ->
                if (attempt > 0) {
                    Thread.sleep(50L * attempt)
                }

                val submittedTransactionId = transactionId.takeIf { attempt > 0 && providerAttemptTimeoutMillis != null }
                if (rejectIfTooLate(paymentId, paymentStartedAt, deadline, submittedTransactionId)) {
                    return
                }

                val semaphoreWaitMillis = (waitUntilMillis - now()).coerceAtLeast(0L)
                if (!semaphore.tryAcquire(semaphoreWaitMillis, TimeUnit.MILLISECONDS)) {
                    rejectIfTooLate(paymentId, paymentStartedAt, deadline, submittedTransactionId)
                    return
                }

                try {
                    if (!rateLimiter.tickBlocking(waitUntilMillis)) {
                        rejectIfTooLate(paymentId, paymentStartedAt, deadline, submittedTransactionId)
                        return
                    }

                    if (rejectIfTooLate(paymentId, paymentStartedAt, deadline, submittedTransactionId)) {
                        return
                    }

                    if (attempt == 0) {
                        paymentESService.update(paymentId) {
                            it.logSubmission(
                                success = true,
                                transactionId,
                                now(),
                                Duration.ofMillis(now() - paymentStartedAt),
                            )
                        }

                        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
                    }

                    val remainingMillis = (deadline - now()).coerceAtLeast(1L)
                    val providerTimeoutMillis = providerAttemptTimeoutMillis?.let {
                        minOf(it, (remainingMillis - RESPONSE_GRACE_MILLIS).coerceAtLeast(1L))
                    }
                    val requestWithTimeout = providerTimeoutMillis?.let {
                        val url = request.url.newBuilder()
                            .addQueryParameter("timeout", Duration.ofMillis(it).toString())
                            .build()
                        request.newBuilder().url(url).build()
                    } ?: request
                    val clientTimeoutMillis = providerTimeoutMillis?.let {
                        minOf(remainingMillis, it + RESPONSE_GRACE_MILLIS)
                    } ?: remainingMillis
                    val call = client.newCall(requestWithTimeout)
                    call.timeout().timeout(clientTimeoutMillis, TimeUnit.MILLISECONDS)

                    paymentMetrics.recordOutgoing(accountName, retryAfter)
                    val startedAt = System.nanoTime()
                    val (status, body) = try {
                        call.execute().use { response ->
                            val responseBody = response.body?.string()
                            val body = try {
                                mapper.readValue(responseBody, ExternalSysResponse::class.java)
                                    ?: throw IOException("Null payment provider response")
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
                            response.code to body
                        }
                    } catch (e: IOException) {
                        val timedOut = e is SocketTimeoutException || (e is InterruptedIOException && call.isCanceled())
                        paymentMetrics.recordCallDuration(
                            accountName,
                            if (timedOut) PaymentCallOutcome.CLIENT_TIMEOUT else PaymentCallOutcome.FAILED,
                            Duration.ofNanos(System.nanoTime() - startedAt),
                        )
                        throw e
                    }

                    val outcome = when {
                        body.result -> PaymentCallOutcome.SUCCESS
                        providerTimeoutMillis != null && status == 408 && body.message == "Timeout" ->
                            PaymentCallOutcome.PROVIDER_TIMEOUT
                        body.message == "Temporary error" -> PaymentCallOutcome.TEMPORARY_ERROR
                        else -> PaymentCallOutcome.FAILED
                    }
                    paymentMetrics.recordCallDuration(accountName, outcome, Duration.ofNanos(System.nanoTime() - startedAt))

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                    )

                    if (outcome.retryable && attempt < 2) {
                        retryAfter = outcome
                        return@repeat
                    }

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
                    return
                } finally {
                    semaphore.release()
                }
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
    }

    private fun rejectIfTooLate(
        paymentId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        submittedTransactionId: UUID? = null,
    ): Boolean {
        val checkedAt = now()
        val remainingMillis = deadline - checkedAt

        if (remainingMillis > paymentTimeReserveMillis) {
            return false
        }

        val transactionId = submittedTransactionId ?: UUID.randomUUID()

        if (submittedTransactionId == null) {
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
