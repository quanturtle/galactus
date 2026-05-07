from galactus.transform.base import Parser

_REGISTRY: dict[str, type[Parser]] = {}


def register_parser(name: str):
    """Decorator that registers a Parser subclass under a string key.

    Usage:
        @register_parser("ultimahora")
        class UltimaHoraParser(Parser): ...
    """

    def decorator(cls: type[Parser]) -> type[Parser]:
        if not issubclass(cls, Parser):
            raise TypeError(f"{cls.__name__} must subclass Parser")
        existing = _REGISTRY.get(name)
        if existing is not None and existing is not cls:
            raise ValueError(f"parser {name!r} already registered to {existing.__name__}")
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_parser(name: str) -> type[Parser]:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"unknown parser {name!r}; registered: {known}") from exc


def registered_parsers() -> list[str]:
    return sorted(_REGISTRY)
