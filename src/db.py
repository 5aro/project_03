import psycopg

from src.config import POSTGRES_DSN


def get_connection():
    """
    PostgreSQL 연결 객체를 반환합니다.

    사용 예:
        with get_connection() as conn:
            ...
    """

    if not POSTGRES_DSN:
        raise RuntimeError("POSTGRES_DSN 환경변수가 설정되지 않았습니다.")

    return psycopg.connect(POSTGRES_DSN)


def execute_sql(sql: str, params=None):
    """
    INSERT, UPDATE, DELETE 등
    결과를 반환할 필요가 없는 SQL을 실행합니다.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                sql,
                params,
            )

        conn.commit()


def fetch_all(sql: str, params=None):
    """
    SELECT 결과를 모두 가져옵니다.

    반환 형태:
        [
            (값1, 값2, ...),
            (값1, 값2, ...)
        ]
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                sql,
                params,
            )

            rows = cur.fetchall()

    return rows


def fetch_one(sql: str, params=None):
    """
    SELECT 결과 중 첫 번째 행 하나를 가져옵니다.

    결과가 없으면 None을 반환합니다.
    """

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                sql,
                params,
            )

            row = cur.fetchone()

    return row
