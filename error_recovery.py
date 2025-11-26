"""
Error recovery and retry mechanisms for Data Sanitizer.

Provides:
- Retry decorators with exponential backoff
- Circuit breaker pattern
- Graceful degradation
- Error recovery strategies
"""

import functools
import logging
import time
from enum import Enum
from typing import Any, Callable, Optional, Tuple, Type

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry strategies."""

    EXPONENTIAL_BACKOFF = "exponential"
    LINEAR_BACKOFF = "linear"
    FIXED_DELAY = "fixed"


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted."""

    pass


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None,
):
    """
    Retry decorator with configurable backoff strategy.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for exponential strategy
        strategy: Retry strategy to use
        exceptions: Tuple of exception types to catch
        on_retry: Optional callback function called on each retry

    Example:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        def fetch_data():
            # Code that might fail
            pass
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"Failed after {max_attempts} attempts: {func.__name__}",
                            exc_info=True,
                            extra={"function": func.__name__, "attempts": max_attempts},
                        )
                        raise RetryError(f"Failed after {max_attempts} attempts") from e

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. Retrying in {current_delay}s...",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "delay": current_delay,
                        },
                    )

                    if on_retry:
                        on_retry(attempt, e)

                    time.sleep(current_delay)

                    # Calculate next delay based on strategy
                    if strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
                        current_delay *= backoff
                    elif strategy == RetryStrategy.LINEAR_BACKOFF:
                        current_delay += delay
                    # FIXED_DELAY keeps current_delay unchanged

            raise last_exception

        return wrapper

    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    Prevents cascading failures by stopping calls to a failing service
    and allowing it time to recover.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds to wait before attempting recovery
            expected_exception: Exception type that triggers the circuit breaker
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Call function through circuit breaker.

        Args:
            func: Function to call
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result of function call

        Raises:
            Exception: If circuit is open or function raises exception
        """
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                logger.info("Circuit breaker entering HALF_OPEN state")
                self.state = CircuitState.HALF_OPEN
            else:
                raise Exception("Circuit breaker is OPEN - service unavailable")

        try:
            result = func(*args, **kwargs)

            if self.state == CircuitState.HALF_OPEN:
                logger.info("Circuit breaker recovering - entering CLOSED state")
                self.state = CircuitState.CLOSED
                self.failure_count = 0

            return result

        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            logger.warning(
                f"Circuit breaker failure {self.failure_count}/{self.failure_threshold}",
                extra={"failure_count": self.failure_count, "threshold": self.failure_threshold},
            )

            if self.failure_count >= self.failure_threshold:
                logger.error("Circuit breaker OPENED - too many failures")
                self.state = CircuitState.OPEN

            raise

    def __call__(self, func: Callable) -> Callable:
        """Allow CircuitBreaker to be used as a decorator."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)

        return wrapper

    def reset(self):
        """Manually reset circuit breaker."""
        logger.info("Circuit breaker manually reset")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None


class FallbackHandler:
    """Handle graceful degradation with fallback values."""

    @staticmethod
    def with_fallback(func: Callable, fallback_value: Any, exceptions: Tuple[Type[Exception], ...] = (Exception,)):
        """
        Execute function with fallback value on error.

        Args:
            func: Function to execute
            fallback_value: Value to return on error
            exceptions: Exceptions to catch

        Returns:
            Result of func or fallback_value
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logger.warning(
                    f"Function {func.__name__} failed, using fallback value: {e}",
                    extra={"function": func.__name__, "fallback_value": fallback_value},
                )
                return fallback_value

        return wrapper


def with_timeout(timeout_seconds: float):
    """
    Decorator to add timeout to function execution.

    Note: This is a simple implementation. For production, consider using
    concurrent.futures or signal-based timeouts.

    Args:
        timeout_seconds: Maximum execution time in seconds
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=timeout_seconds)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        f"Function {func.__name__} timed out after {timeout_seconds}s",
                        extra={"function": func.__name__, "timeout": timeout_seconds},
                    )
                    raise TimeoutError(f"{func.__name__} execution exceeded {timeout_seconds}s")

        return wrapper

    return decorator


class ErrorRecovery:
    """Error recovery strategies for common failure scenarios."""

    @staticmethod
    def retry_database_operation(func: Callable, max_attempts: int = 3) -> Callable:
        """Retry decorator specifically for database operations."""
        return retry(
            max_attempts=max_attempts,
            delay=0.5,
            backoff=2.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            exceptions=(ConnectionError, TimeoutError),
        )(func)

    @staticmethod
    def retry_network_operation(func: Callable, max_attempts: int = 5) -> Callable:
        """Retry decorator specifically for network operations."""
        return retry(
            max_attempts=max_attempts,
            delay=1.0,
            backoff=2.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            exceptions=(ConnectionError, TimeoutError, OSError),
        )(func)

    @staticmethod
    def retry_file_operation(func: Callable, max_attempts: int = 3) -> Callable:
        """Retry decorator specifically for file operations."""
        return retry(
            max_attempts=max_attempts,
            delay=0.1,
            backoff=1.5,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            exceptions=(IOError, OSError),
        )(func)


# Example usage patterns
if __name__ == "__main__":
    # Example 1: Simple retry
    @retry(max_attempts=3, delay=1.0)
    def fetch_data():
        print("Fetching data...")
        import random

        if random.random() < 0.7:
            raise ConnectionError("Network error")
        return "Success!"

    # Example 2: Circuit breaker
    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)

    @circuit_breaker
    def call_external_service():
        print("Calling external service...")
        import random

        if random.random() < 0.8:
            raise Exception("Service error")
        return "Success!"

    # Example 3: Fallback
    @FallbackHandler.with_fallback(lambda: None, fallback_value={"data": []})
    def get_config():
        raise Exception("Config not found")

    # Run examples
    try:
        result = fetch_data()
        print(f"Result: {result}")
    except RetryError as e:
        print(f"Failed: {e}")
