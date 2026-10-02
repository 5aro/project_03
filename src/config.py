import os

MUJI_API_URL = "https://mujikorea.co.kr" "/api/bff/api/products/search/list"

MUJI_CATEGORIES = {
    2: "의복",
    3: "생활",
    4: "식품",
    178: "뷰티",
}

MUJI_DISPLAY_SIZE = 16

REQUEST_TIMEOUT = 15
REQUEST_MAX_RETRIES = 3
REQUEST_DELAY = 0.2


POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/muji"
)


REQUEST_HEADERS = {"User-Agent": ("Mozilla/5.0 " "(MUJI Korea Data Pipeline Project)")}
