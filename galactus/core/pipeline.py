from galactus.core.interfaces import PipelineStage


class Pipeline:
    """Composition root — owns an ordered list of stages and runs them in order.

    Stages satisfy `PipelineStage` (a `name: str` and `async def run(*, source)`).
    Adding a 4th stage means appending it to `stages`; dispatch is by name.
    The optional `source` filter is threaded through to each stage so a single
    source can be run end-to-end.
    """

    def __init__(self, *, stages: list[PipelineStage]) -> None:
        self.stages = stages
        self._by_name = {s.name: s for s in stages}
        if len(self._by_name) != len(stages):
            raise ValueError("duplicate stage names in pipeline")

    async def run(self, *, source: str | None = None, stage_name: str | None = None) -> None:
        if stage_name is None:
            for stage in self.stages:
                await stage.run(source=source)
            return
        try:
            stage = self._by_name[stage_name]
        except KeyError:
            known = ", ".join(s.name for s in self.stages) or "<none>"
            raise ValueError(f"unknown stage {stage_name!r}; available: {known}") from None
        await stage.run(source=source)
        return
