"""
Пример: Оформление заказа с оплатой и отправкой уведомления.
Сценарий:
1. Создаем заказ в БД.
2. Пытаемся списать деньги (может упасть).
3. Отправляем email (может упасть).

Если шаг 2 или 3 падает -> Заказ автоматически отменяется (компенсация).
Если сервис оплаты «лег» полностью -> Circuit Breaker прекратит попытки, чтобы не добивать его.
"""

import asyncio
import random
from flowguard import Pipeline, JSONStore, CircuitBreaker

# --- Имитация внешних сервисов ---
class Database:
    orders = {}
    def create_order(self, item):
        order_id = len(self.orders) + 1
        self.orders[order_id] = {"item": item, "status": "PENDING"}
        print(f"📦 Заказ #{order_id} создан в БД.")
        return order_id
    
    def cancel_order(self, order_id):
        if order_id in self.orders:
            self.orders[order_id]["status"] = "CANCELED"
            print(f"❌ Заказ #{order_id} ОТМЕНЕН (компенсация).")

class PaymentGateway:
    def charge(self, amount):
        if random.random() > 0.8: # 20% шанс ошибки
            raise ConnectionError("Шлюз оплаты недоступен")
        print(f"💰 Оплата {amount}$ прошла успешно.")

class EmailService:
    def send(self, email, text):
        if random.random() > 0.5: # 50% шанс ошибки
            raise TimeoutError("SMTP сервер не отвечает")
        print(f"📧 Письмо отправлено на {email}: '{text}'")

db = Database()
payment = PaymentGateway()
email = EmailService()

# --- Настройка пайплайна ---
# Используем JSONStore, чтобы прогресс сохранялся на диск (файл state.json)
pipeline = Pipeline(
    name="checkout_process",
    store=JSONStore("checkout_state.json"),
    on_failure=lambda ctx, err: print(f"🚨 КРИТИЧЕСКИЙ СБОЙ: {err}. Менеджер уведомлен.")
)

# Шаг 1: Создание заказа (без компенсации, так как это начало)
@pipeline.step()
def step_create_order(ctx):
    order_id = db.create_order(ctx["item"])
    return {"order_id": order_id}

# Шаг 2: Оплата (с компенсацией отмены заказа и Circuit Breaker)
# Если оплата падает 3 раза подряд -> цепь размыкается
cb_payment = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

@pipeline.step(
    compensate=lambda ctx: db.cancel_order(ctx["order_id"]),
    retries=2, 
    backoff='exponential',
    circuit_breaker=cb_payment
)
def step_payment(ctx):
    payment.charge(100)
    return {"payment_status": "PAID"}

# Шаг 3: Отправка письма (с компенсацией... ну например, логированием ошибки)
@pipeline.step(
    compensate=lambda ctx: print(f"⚠️ Не удалось отправить письмо для заказа {ctx['order_id']}"),
    retries=1
)
def step_notify(ctx):
    email.send("client@mail.com", f"Ваш заказ {ctx['order_id']} оплачен!")
    return {"notified": True}

# --- Запуск ---
async def main():
    print("🚀 Запуск процесса оформления заказа...")
    try:
        await pipeline.run({"item": "iPhone 15 Pro"})
        print("✅ Заказ успешно оформлен!")
    except Exception as e:
        print(f"⛔ Процесс прерван: {e}")

if __name__ == "__main__":
    asyncio.run(main())