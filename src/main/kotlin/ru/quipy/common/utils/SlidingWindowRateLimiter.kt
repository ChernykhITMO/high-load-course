package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

/** Limits admissions in every rolling time window, including at window boundaries. */
class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
) : RateLimiter {
    private val windowMillis = window.toMillis()
    private val monitor = java.lang.Object()
    private val admittedAt = ArrayDeque<Long>()

    init {
        require(rate > 0) { "Rate must be positive" }
        require(windowMillis > 0) { "Window must be at least one millisecond" }
    }

    override fun tick(): Boolean = synchronized(monitor) {
        val currentTime = System.currentTimeMillis()
        discardExpired(currentTime)
        if (admittedAt.size.toLong() >= rate) {
            false
        } else {
            admittedAt.addLast(currentTime)
            true
        }
    }

    fun tickBlocking(deadlineMillis: Long): Boolean = synchronized(monitor) {
        while (true) {
            val currentTime = System.currentTimeMillis()
            if (currentTime >= deadlineMillis) return@synchronized false

            discardExpired(currentTime)
            if (admittedAt.size.toLong() < rate) {
                admittedAt.addLast(currentTime)
                return@synchronized true
            }

            val nextSlotAt = admittedAt.first + windowMillis
            val waitMillis = minOf(deadlineMillis, nextSlotAt) - currentTime
            monitor.wait(waitMillis.coerceAtLeast(1L))
        }

        @Suppress("UNREACHABLE_CODE")
        false
    }

    private fun discardExpired(currentTime: Long) {
        while (admittedAt.isNotEmpty() && currentTime - admittedAt.first >= windowMillis) {
            admittedAt.removeFirst()
        }
    }
}
