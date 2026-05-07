from galactus.extract.base import Scraper

_REGISTRY: dict[str, type[Scraper]] = {}


def register_scraper(name: str):
    """Decorator that registers a Scraper subclass under a string key.

    Usage:
        @register_scraper("ultimahora")
        class UltimaHoraScraper(BfsScraper): ...
    """

    def decorator(cls: type[Scraper]) -> type[Scraper]:
        if not issubclass(cls, Scraper):
            raise TypeError(f"{cls.__name__} must subclass Scraper")
        existing = _REGISTRY.get(name)
        if existing is not None and existing is not cls:
            raise ValueError(f"scraper {name!r} already registered to {existing.__name__}")
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_scraper(name: str) -> type[Scraper]:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"unknown scraper {name!r}; registered: {known}") from exc


def registered_scrapers() -> list[str]:
    return sorted(_REGISTRY)
