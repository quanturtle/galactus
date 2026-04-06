import importlib
import inspect
import pkgutil
from pathlib import Path


def _discover_parsers() -> tuple[dict, dict]:
    html_parsers: dict[str, callable] = {}
    api_parsers: dict[str, callable] = {}
    package_dir = Path(__file__).parent
    for _, name, _ in pkgutil.iter_modules([str(package_dir)]):
        if name.startswith("_"):
            continue
        module = importlib.import_module(f"{__package__}.{name}")
        parse_fn = getattr(module, "parse", None)
        if parse_fn is None:
            continue
        source = getattr(module, "SOURCE", name)
        sig = inspect.signature(parse_fn)
        if len(sig.parameters) >= 2:
            html_parsers[source] = parse_fn
        else:
            api_parsers[source] = parse_fn
    return html_parsers, api_parsers


HTML_PARSERS, API_PARSERS = _discover_parsers()


def parse_snapshot(source: str, html: str, url: str) -> dict | None:
    parser = HTML_PARSERS.get(source)
    if parser is None:
        raise ValueError(f"No HTML parser registered for source: {source}")
    return parser(html, url)


def parse_api_response(source: str, response_text: str) -> list[dict]:
    parser = API_PARSERS.get(source)
    if parser is None:
        raise ValueError(f"No API parser registered for source: {source}")
    return parser(response_text)
