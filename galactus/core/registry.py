from collections.abc import Callable
from functools import partial


def _register_cls(
    items: dict[str, type],
    base: type,
    label: str,
    name: str,
    cls: type,
) -> type:
    if not issubclass(cls, base):
        raise TypeError(f"{cls.__name__} must subclass {base.__name__}")
    existing = items.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(f"{label} {name!r} already registered to {existing.__name__}")
    items[name] = cls
    return cls


class ClassRegistry[T]:
    """Name → type[T] mapping with a decorator-style register method.

    Usage:
        SCRAPERS = ClassRegistry[Scraper]("scraper", base=Scraper)

        @SCRAPERS.register("ultimahora")
        class UltimaHoraScraper(BfsScraper): ...

        cls = SCRAPERS.get("ultimahora")
    """

    def __init__(self, label: str, base: type[T]) -> None:
        self._label = label
        self._base = base
        self._items: dict[str, type[T]] = {}

    def register(self, name: str) -> Callable[[type[T]], type[T]]:
        return partial(_register_cls, self._items, self._base, self._label, name)

    def get(self, name: str) -> type[T]:
        try:
            return self._items[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._items)) or "<none>"
            raise KeyError(f"unknown {self._label} {name!r}; registered: {known}") from exc

    def names(self) -> list[str]:
        return sorted(self._items)
