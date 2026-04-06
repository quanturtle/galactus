from noticias.parsers import (
    abc_color,
    adndigital,
    cronica,
    elnacional,
    hoy,
    lanacion,
    latribuna,
    megacadena,
    npy,
    ultimahora,
)

HTML_PARSERS: dict[str, object] = {
    "ultimahora": ultimahora,
    "npy": npy,
    "cronica": cronica,
    "elnacional": elnacional,
    "adndigital": adndigital,
}

API_PARSERS: dict[str, object] = {
    "abc": abc_color,
    "lanacion": lanacion,
    "latribuna": latribuna,
    "hoy": hoy,
    "megacadena": megacadena,
}


def parse_snapshot(source: str, html: str, url: str) -> dict | None:
    parser = HTML_PARSERS[source]
    return parser.parse(html, url)


def parse_api_response(source: str, response_text: str) -> list[dict]:
    parser = API_PARSERS[source]
    return parser.parse(response_text)
