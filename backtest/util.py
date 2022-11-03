def cagr(initial: float, final: float, duration: float):
    return (final / initial) ** (1 / duration) - 1


def rate_of_return(initial: float, final: float):
    return (final - initial) / initial


def P(target: str):
    return of("P", target)


def R(target: str):
    return of("R", target)


def of(operation: str, subject: str):
    return f"{operation}({subject})"