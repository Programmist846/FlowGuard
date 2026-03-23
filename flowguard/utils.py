import asyncio
import random

async def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0):
    """Экспоненциальная задержка с джиттером."""
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0, 0.1 * delay)
    await asyncio.sleep(delay + jitter)