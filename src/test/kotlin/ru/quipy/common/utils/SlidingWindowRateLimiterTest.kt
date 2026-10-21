package ru.quipy.common.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SlidingWindowRateLimiterTest {
    @Test
    fun `only the configured number of concurrent requests enter a window`() {
        val limiter = SlidingWindowRateLimiter(5, Duration.ofSeconds(5))
        val workers = Executors.newFixedThreadPool(32)
        try {
            val results = workers.invokeAll(List(32) { Callable { limiter.tick() } })
            assertEquals(5, results.count { it.get() })
        } finally {
            workers.shutdownNow()
        }
    }

    @Test
    fun `waiting stops at the deadline`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(5))
        assertEquals(true, limiter.tick())
        val startedAt = System.nanoTime()

        assertFalse(limiter.tickBlocking(System.currentTimeMillis() + 50))

        val waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
        assertEquals(true, waitedMillis < 1000)
    }
}
