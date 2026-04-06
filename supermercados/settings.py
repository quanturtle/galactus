import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://the_scraper:the_scraper_secret@localhost:5432/the_scraper",
)
