from noticias.parsers._wordpress_ld import make_parser

SOURCE = "elnacional"

parse = make_parser(
    SOURCE,
    ["article", ".entry-content", ".article-content", ".post-content"],
)
