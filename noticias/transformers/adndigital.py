from noticias.transformers._wordpress_ld import make_transformer

SOURCE = "adndigital"

transform = make_transformer(
    SOURCE,
    ["article", ".entry-content", ".post-content"],
)
