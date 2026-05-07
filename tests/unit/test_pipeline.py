import asyncio
from dataclasses import dataclass, field

import pytest

from galactus.core.pipeline import Pipeline


@dataclass
class FakeStage:
    name: str
    runs: list[tuple[str, str | None]] = field(default_factory=list)
    delay: float = 0.0

    async def run(self, *, source: str | None = None) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        self.runs.append((self.name, source))
        return


def test_pipeline_runs_all_stages_in_order() -> None:
    a = FakeStage("a")
    b = FakeStage("b")
    c = FakeStage("c")
    pipeline = Pipeline(stages=[a, b, c])

    asyncio.run(pipeline.run())

    assert a.runs == [("a", None)]
    assert b.runs == [("b", None)]
    assert c.runs == [("c", None)]


def test_pipeline_runs_single_stage_by_name() -> None:
    a = FakeStage("a")
    b = FakeStage("b")
    pipeline = Pipeline(stages=[a, b])

    asyncio.run(pipeline.run(stage_name="b"))

    assert a.runs == []
    assert b.runs == [("b", None)]


def test_pipeline_threads_source_filter_to_stages() -> None:
    a = FakeStage("a")
    b = FakeStage("b")
    pipeline = Pipeline(stages=[a, b])

    asyncio.run(pipeline.run(source="ultimahora"))

    assert a.runs == [("a", "ultimahora")]
    assert b.runs == [("b", "ultimahora")]


def test_pipeline_threads_source_filter_to_single_stage() -> None:
    a = FakeStage("a")
    b = FakeStage("b")
    pipeline = Pipeline(stages=[a, b])

    asyncio.run(pipeline.run(source="ultimahora", stage_name="b"))

    assert a.runs == []
    assert b.runs == [("b", "ultimahora")]


def test_pipeline_unknown_stage_name_raises_with_available_list() -> None:
    pipeline = Pipeline(stages=[FakeStage("a"), FakeStage("b")])

    with pytest.raises(ValueError) as excinfo:
        asyncio.run(pipeline.run(stage_name="missing"))

    msg = str(excinfo.value)
    assert "missing" in msg
    assert "a" in msg and "b" in msg


def test_pipeline_rejects_duplicate_stage_names() -> None:
    with pytest.raises(ValueError, match="duplicate stage names"):
        Pipeline(stages=[FakeStage("a"), FakeStage("a")])
