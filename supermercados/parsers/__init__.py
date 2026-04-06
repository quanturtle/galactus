from supermercados.parsers import arete, biggie, casarica, grutter, superseis

HTML_PARSERS = {
    "arete": arete.parse,
    "casarica": casarica.parse,
    "superseis": superseis.parse,
}

API_PARSERS = {
    "biggie": biggie.parse,
    "grutter": grutter.parse,
}


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
