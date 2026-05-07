from datetime import datetime

from pydantic import BaseModel, Field


class Article(BaseModel):
    """Silver entity: a single news article.

    Persistence is handled by SilverRepo, not by this model.
    """

    source: str
    source_url: str
    title: str
    body_html: str | None = None
    body_text: str | None = None
    authors: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    section: str | None = None
    tags: list[str] = Field(default_factory=list)
    image_urls: list[str] = Field(default_factory=list)
