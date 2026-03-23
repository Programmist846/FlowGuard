import asyncio
import random
from flowguard import Pipeline, MemoryStore, CircuitBreaker

# Имитация базы данных
db_state = {"users": []}

def create_user(ctx):
    user_id = len(db_state["users"]) + 1
    db_state["users"].append({"id": user_id, "email": ctx["email"]})
    print(f"✅ User created: {user_id}")
    return {"user_id": user_id}

def delete_user(ctx):
    print(f"🔄 Compensating: Deleting user {ctx.get('user_id')}")
    # Логика удаления

def send_email(ctx):
    if random.random() > 0.7: # 30% шанс ошибки
        raise ConnectionError("SMTP Server Down")
    print(f"📧 Email sent to {ctx['email']}")

async def main():
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=10)
    
    pipeline = Pipeline(
        name="onboarding",
        store=MemoryStore(),
        on_failure=lambda ctx, err: print(f"🚨 Critical Failure: {err}")
    )

    @pipeline.step()
    def step1(ctx):
        return create_user(ctx)

    @pipeline.step(compensate=delete_user, retries=2, backoff='exponential', circuit_breaker=cb)
    def step2(ctx):
        return send_email(ctx)

    try:
        await pipeline.run({"email": "test@example.com"})
    except Exception:
        print("Pipeline execution failed.")

if __name__ == "__main__":
    asyncio.run(main())