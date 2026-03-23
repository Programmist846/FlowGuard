import asyncio
import logging
import time
import inspect # Добавили inspect
from typing import Any, Callable, Dict, List, Optional, Union
from functools import wraps, update_wrapper

from .exceptions import CircuitBreakerOpen, StepExecutionError
from .utils import exponential_backoff
from .store import BaseStore, MemoryStore

logger = logging.getLogger(__name__)

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time: Optional[float] = None
        self.state = "CLOSED"

    def record_success(self):
        self.failures = 0
        self.state = "CLOSED"

    def record_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit Breaker OPENED after {self.failures} failures.")

    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.recovery_timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit Breaker HALF_OPEN. Testing...")
                return True
            return False
        return True

class Step:
    def __init__(self, func: Callable, compensate: Optional[Callable] = None, 
                 retries: int = 0, backoff: str = 'linear', circuit_breaker: Optional[CircuitBreaker] = None):
        self.func = func
        self.compensate = compensate
        self.retries = retries
        self.backoff = backoff
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.name = func.__name__
        # Сохраняем метаданные функции для корректной работы декораторов
        update_wrapper(self, func)

    async def execute(self, context: Dict[str, Any]) -> Any:
        if not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpen(f"Circuit breaker is OPEN for step {self.name}")

        attempt = 0
        while True:
            try:
                logger.info(f"Executing step: {self.name} (attempt {attempt + 1})")
                # Исправлено на inspect
                if inspect.iscoroutinefunction(self.func):
                    result = await self.func(context)
                else:
                    result = self.func(context)
                
                self.circuit_breaker.record_success()
                return result
            except Exception as e:
                self.circuit_breaker.record_failure()
                if attempt < self.retries:
                    logger.warning(f"Step {self.name} failed: {e}. Retrying...")
                    if self.backoff == 'exponential':
                        await exponential_backoff(attempt)
                    else:
                        await asyncio.sleep(1)
                    attempt += 1
                else:
                    logger.error(f"Step {self.name} failed permanently: {e}")
                    raise StepExecutionError(f"Step {self.name} failed after {attempt + 1} attempts") from e

class Pipeline:
    def __init__(self, name: str, store: Optional[BaseStore] = None, 
                 on_failure: Optional[Callable] = None):
        self.name = name
        self.store = store or MemoryStore()
        self.on_failure = on_failure
        self.steps: List[Step] = []
        # Словарь для быстрого доступа к шагам по имени функции (для тестов)
        self.step_map: Dict[str, Step] = {} 

    def step(self, compensate: Optional[Callable] = None, retries: int = 0, 
             backoff: str = 'linear', circuit_breaker: Optional[CircuitBreaker] = None):
        def decorator(func: Callable):
            step_obj = Step(func, compensate, retries, backoff, circuit_breaker)
            self.steps.append(step_obj)
            self.step_map[func.__name__] = step_obj # Сохраняем в мапу
            
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Если вызывают просто как функцию внутри кода, а не через пайплайн
                # Это запасной вариант, но в пайплайне мы используем self.steps
                if inspect.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)
            
            # Хак для тестов: добавляем метод execute к обертке, чтобы можно было вызвать wrapper.execute()
            # Но лучше в тестах обращаться через pipeline.step_map
            wrapper._step_obj = step_obj 
            return wrapper
        return decorator

    async def run(self, initial_context: Dict[str, Any]) -> Dict[str, Any]:
        state_key = f"pipeline:{self.name}"
        context = initial_context.copy()
        executed_steps: List[str] = []

        saved_state = await self.store.get(state_key)
        if saved_state:
            logger.info(f"Resuming pipeline {self.name} from checkpoint.")
            context.update(saved_state.get('context', {}))
            executed_steps = saved_state.get('executed_steps', [])
        else:
            logger.info(f"Starting new pipeline run: {self.name}")

        try:
            for step in self.steps:
                if step.name in executed_steps:
                    logger.info(f"Skipping already executed step: {step.name}")
                    continue

                try:
                    result = await step.execute(context)
                    if isinstance(result, dict):
                        context.update(result)
                    
                    executed_steps.append(step.name)
                    await self.store.set(state_key, {
                        'context': context,
                        'executed_steps': executed_steps
                    })
                except Exception as e:
                    logger.error(f"Pipeline failed at step {step.name}. Initiating compensation...")
                    await self._compensate(executed_steps, context)
                    if self.on_failure:
                        await self._call_on_failure(context, e)
                    raise
            
            await self.store.delete(state_key)
            logger.info(f"Pipeline {self.name} completed successfully.")
            return context

        except Exception as e:
            raise e

    async def _compensate(self, executed_steps: List[str], context: Dict[str, Any]):
        for step_name in reversed(executed_steps):
            step = next((s for s in self.steps if s.name == step_name), None)
            if step and step.compensate:
                try:
                    logger.info(f"Compensating step: {step_name}")
                    if inspect.iscoroutinefunction(step.compensate):
                        await step.compensate(context)
                    else:
                        step.compensate(context)
                except Exception as e:
                    logger.error(f"Compensation for {step_name} failed: {e}")

    async def _call_on_failure(self, context: Dict[str, Any], error: Exception):
        if self.on_failure:
            if inspect.iscoroutinefunction(self.on_failure):
                await self.on_failure(context, error)
            else:
                self.on_failure(context, error)