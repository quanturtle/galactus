from noticias.transformers._wordpress_ld import make_transformer

SOURCE = "elnacional"

transform = make_transformer(
    SOURCE,
    ["article", ".entry-content", ".article-content", ".post-content"],
)
