from noticias.parsers._wordpress_ld import make_parser

SOURCE = "adndigital"

parse = make_parser(
    SOURCE,
    ["article", ".entry-content", ".post-content"],
)
