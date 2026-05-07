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

    def register(self, name: str):
        def decorator(cls: type[T]) -> type[T]:
            if not issubclass(cls, self._base):
                raise TypeError(f"{cls.__name__} must subclass {self._base.__name__}")
            existing = self._items.get(name)
            if existing is not None and existing is not cls:
                raise ValueError(
                    f"{self._label} {name!r} already registered to {existing.__name__}"
                )
            self._items[name] = cls
            return cls

        return decorator

    def get(self, name: str) -> type[T]:
        try:
            return self._items[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._items)) or "<none>"
            raise KeyError(f"unknown {self._label} {name!r}; registered: {known}") from exc

    def names(self) -> list[str]:
        return sorted(self._items)
