class CircuitBreakerOpen(Exception):
    """Вызывается, когда цепь разомкнута из-за частых ошибок."""
    pass

class StepExecutionError(Exception):
    """Ошибка выполнения шага."""
    pass