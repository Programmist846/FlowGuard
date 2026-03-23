import asyncio
import pytest
from flowguard import Pipeline, MemoryStore, CircuitBreaker, CircuitBreakerOpen

db_state = {"users": [], "orders": []}

def reset_db():
    db_state["users"] = []
    db_state["orders"] = []

@pytest.mark.asyncio
async def test_successful_pipeline():
    reset_db()
    pipeline = Pipeline(name="test_success", store=MemoryStore())

    @pipeline.step()
    def create_user(ctx):
        db_state["users"].append(ctx["email"])
        return {"user_id": 1}

    @pipeline.step()
    def create_order(ctx):
        db_state["orders"].append(ctx["user_id"])
        return {"order_id": 100}

    result = await pipeline.run({"email": "test@test.com"})
    
    assert result["order_id"] == 100
    assert len(db_state["users"]) == 1
    assert len(db_state["orders"]) == 1

@pytest.mark.asyncio
async def test_compensation_on_failure():
    reset_db()
    pipeline = Pipeline(name="test_compensate_simple", store=MemoryStore())

    compensation_called = False

    def compensate_create_user(ctx):
        nonlocal compensation_called
        compensation_called = True
        db_state["users"].clear()
        # print("DEBUG: Compensation for create_user called!") # Можно оставить для отладки

    @pipeline.step(compensate=compensate_create_user)
    def create_user(ctx):
        db_state["users"].append("user")
        return {}

    @pipeline.step()
    def fail_step(ctx):
        raise Exception("Fail")

    try:
        await pipeline.run({})
    except:
        pass

    # ПРОВЕРКИ
    assert compensation_called, "Компенсация НЕ была вызвана!"
    assert len(db_state["users"]) == 0, f"Список не очищен: {db_state['users']}"
    
    # !!! ЗДЕСЬ НЕ ДОЛЖНО БЫТЬ НИКАКОГО КОДА !!!
    # Удалите строку @pipeline.step(compensate=compensate_user), если она там есть

@pytest.mark.asyncio
async def test_circuit_breaker():
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10)
    pipeline = Pipeline(name="test_cb", store=MemoryStore())

    call_count = 0

    @pipeline.step(circuit_breaker=cb)
    def unstable_service(ctx):
        nonlocal call_count
        call_count += 1
        raise Exception("Service Down")

    step_obj = pipeline.step_map['unstable_service']

    for _ in range(2):
        try:
            await step_obj.execute({})
        except: pass

    with pytest.raises(CircuitBreakerOpen):
        await step_obj.execute({})
    
    assert call_count == 2