from abc import ABC, abstractmethod


class PipelineStage(ABC):
    """Base for pipeline stages. Subclasses set a `name: str` class attribute
    and implement `async def run()`. Pipeline holds an ordered list of these
    and dispatches by `name`."""

    name: str

    @abstractmethod
    async def run(self) -> None: ...


class Pipeline:
    """Composition root — owns an ordered list of stages and runs them in order.

    Adding a 4th stage means appending it to `stages`; dispatch is by name.
    Empty `stage_name` means all stages run in order.
    """

    def __init__(self, *, stages: list[PipelineStage]) -> None:
        self.stages = stages
        self._stage_index = {s.name: s for s in stages}
        if not stages:
            raise ValueError("pipeline needs at least one stage")
        if len(self._stage_index) != len(stages):
            raise ValueError("duplicate stage names in pipeline")

    async def run(self, *, stage_name: str | None = None) -> None:
        if stage_name is None:
            for stage in self.stages:
                await stage.run()
            return
        try:
            stage = self._stage_index[stage_name]
        except KeyError:
            known = ", ".join(s.name for s in self.stages) or "<none>"
            raise ValueError(f"unknown stage {stage_name!r}; available: {known}") from None
        await stage.run()
        return
