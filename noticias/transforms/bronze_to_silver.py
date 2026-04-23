from galactus.transforms import router
from noticias.parsers import API_PARSERS, HTML_PARSERS
from noticias.transforms import api_responses, snapshots


async def run(source: str | None = None) -> int:
    return await router.run(
        html_parsers=HTML_PARSERS,
        api_parsers=API_PARSERS,
        snapshot_runner=snapshots.run,
        api_runner=api_responses.run,
        source=source,
    )
