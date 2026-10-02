import hashlib
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from psycopg.types.json import Jsonb

from src.config import (
    MUJI_API_URL,
    MUJI_CATEGORY_ID,
    MUJI_DISPLAY_SIZE,
    REQUEST_DELAY,
    REQUEST_HEADERS,
    REQUEST_MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from src.db import get_connection

# ============================================================
# 1. MUJI API 한 페이지 요청
# ============================================================


def fetch_page(category_id: int, page: int) -> dict:
    """
    MUJI API에서 특정 페이지의 상품 데이터를 가져온다.

    요청 실패 시 지수 백오프 방식으로 자동 재시도한다.
    예:
        1차 실패 -> 1초 대기
        2차 실패 -> 2초 대기
        3차 실패 -> 최종 실패
    """

    params = {
        "sort": "NEWDESC",
        "display_size": MUJI_DISPLAY_SIZE,
        "category_id": category_id,
        "exclude_soldout": 0,
        "page": page,
    }

    last_error = None

    for attempt in range(REQUEST_MAX_RETRIES):
        try:
            response = requests.get(
                MUJI_API_URL,
                params=params,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            result = response.json()

            if not result.get("success"):
                raise RuntimeError("MUJI API 응답 실패: " f"{result.get('message')}")

            return result

        except (
            requests.RequestException,
            ValueError,
            RuntimeError,
        ) as error:

            last_error = error

            if attempt < REQUEST_MAX_RETRIES - 1:
                delay = 2**attempt

                print(
                    "[재시도] "
                    f"page={page}, "
                    f"attempt={attempt + 1}, "
                    f"delay={delay}s"
                )

                time.sleep(delay)

    raise RuntimeError(
        "MUJI API 요청 최종 실패 "
        f"category_id={category_id}, "
        f"page={page}: "
        f"{last_error}"
    )


# ============================================================
# 2. Raw JSON Hash 생성
# ============================================================


def make_content_hash(data: dict) -> str:
    """
    동일한 JSON 데이터인지 확인하기 위한 SHA256 Hash를 생성한다.
    """

    json_string = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
    )

    return hashlib.sha256(json_string.encode("utf-8")).hexdigest()


# ============================================================
# 3. Raw 데이터 PostgreSQL 저장
# ============================================================


def save_raw_page(
    batch_id: str,
    query_category_id: int,
    response_category_id: int | None,
    response_category_name: str | None,
    page: int,
    response_data: dict,
):
    """
    MUJI API 원본 응답을 muji_raw 테이블에 저장한다.
    """

    content_hash = make_content_hash(response_data)
    collected_at = datetime.now(timezone.utc)

    sql = """
        INSERT INTO muji_raw (
            batch_id,
            query_category_id,
            response_category_id,
            response_category_name,
            page,
            collected_at,
            raw_json,
            content_hash
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (
            batch_id,
            query_category_id,
            page
        )
        DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    batch_id,
                    query_category_id,
                    response_category_id,
                    response_category_name,
                    page,
                    collected_at,
                    Jsonb(response_data),
                    content_hash,
                ),
            )

        conn.commit()


# ============================================================
# 4. 여러 페이지 순차 수집
# ============================================================


def fetch_pages_sequential(
    category_id: int,
    pages: list[int],
) -> dict[int, dict]:
    """
    페이지를 하나씩 순서대로 요청한다.

    ThreadPoolExecutor와 성능 비교를 위한 함수이다.
    """

    results = {}

    for page in pages:
        results[page] = fetch_page(
            category_id=category_id,
            page=page,
        )

        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    return results


# ============================================================
# 5. 여러 페이지 동시 수집
# ============================================================


def fetch_pages_concurrent(
    category_id: int,
    pages: list[int],
    max_workers: int = 5,
) -> dict[int, dict]:
    """
    ThreadPoolExecutor를 사용해 여러 페이지를 동시에 요청한다.

    HTTP 요청은 I/O Bound 작업이므로
    ThreadPoolExecutor를 사용하면 순차 요청보다
    대기 시간을 줄일 수 있다.
    """

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        future_to_page = {
            executor.submit(
                fetch_page,
                category_id,
                page,
            ): page
            for page in pages
        }

        for future in as_completed(future_to_page):
            page = future_to_page[future]

            try:
                results[page] = future.result()

                print("[동시 수집] " f"page={page}")

            except Exception as error:
                raise RuntimeError(
                    f"페이지 수집 실패 page={page}: " f"{error}"
                ) from error

    return results


# ============================================================
# 6. 순차 / 동시 수집 성능 비교
# ============================================================


def compare_collection_speed(
    category_id: int = MUJI_CATEGORY_ID,
    test_pages: int = 10,
    max_workers: int = 5,
) -> dict:
    """
    동일한 페이지를 대상으로

    1. 순차 수집
    2. ThreadPoolExecutor 동시 수집

    시간을 측정해 비교한다.

    실제 DB에는 저장하지 않는다.
    """

    pages = list(range(1, test_pages + 1))

    print()
    print("=" * 60)
    print("순차 / 동시 수집 성능 비교")
    print("=" * 60)

    # ------------------------------
    # 순차 수집
    # ------------------------------

    print()
    print("[순차 수집 시작]")

    sequential_start = time.perf_counter()

    sequential_results = fetch_pages_sequential(
        category_id=category_id,
        pages=pages,
    )

    sequential_time = time.perf_counter() - sequential_start

    print("[순차 수집 완료] " f"{sequential_time:.2f}초")

    # ------------------------------
    # 동시 수집
    # ------------------------------

    print()
    print("[동시 수집 시작]")

    concurrent_start = time.perf_counter()

    concurrent_results = fetch_pages_concurrent(
        category_id=category_id,
        pages=pages,
        max_workers=max_workers,
    )

    concurrent_time = time.perf_counter() - concurrent_start

    print("[동시 수집 완료] " f"{concurrent_time:.2f}초")

    # ------------------------------
    # 결과 검증
    # ------------------------------

    if len(sequential_results) != len(concurrent_results):
        raise RuntimeError("순차/동시 수집 결과의 페이지 수가 다릅니다.")

    speedup = sequential_time / concurrent_time if concurrent_time > 0 else 0

    print()
    print("=" * 60)
    print("성능 비교 결과")
    print("=" * 60)

    print(f"테스트 페이지 : {test_pages}")

    print(f"순차 수집     : {sequential_time:.2f}초")

    print(f"동시 수집     : {concurrent_time:.2f}초")

    print(f"속도 향상     : {speedup:.2f}배")

    return {
        "test_pages": test_pages,
        "max_workers": max_workers,
        "sequential_time": round(
            sequential_time,
            2,
        ),
        "concurrent_time": round(
            concurrent_time,
            2,
        ),
        "speedup": round(
            speedup,
            2,
        ),
    }


# ============================================================
# 7. 실제 Extract
# ============================================================


def extract_category(
    category_id: int = MUJI_CATEGORY_ID,
    max_workers: int = 5,
) -> dict:
    """
    MUJI 생활 카테고리 전체 상품을 수집한다.

    첫 페이지:
        전체 페이지 수 확인을 위해 먼저 요청

    2페이지 이후:
        ThreadPoolExecutor로 동시에 요청

    수집된 Raw JSON:
        PostgreSQL muji_raw 테이블에 저장
    """

    batch_id = str(uuid.uuid4())

    print("[Extract 시작] " f"batch_id={batch_id}")

    started_at = time.perf_counter()

    # --------------------------------------------------------
    # 첫 페이지 수집
    # --------------------------------------------------------

    first_response = fetch_page(
        category_id=category_id,
        page=1,
    )

    first_data = first_response["data"]

    paginate = first_data["paginate"]

    last_page = paginate["last_page"]
    total_products = paginate["total"]

    category_info = first_data.get("categoryInfo") or {}

    response_category_id = category_info.get("category_id")

    response_category_name = category_info.get("name")

    print("[요청 카테고리] " f"{category_id}")

    print("[응답 카테고리] " f"{response_category_id} " f"{response_category_name}")

    print("[상품 수] " f"{total_products:,}")

    print("[페이지 수] " f"{last_page}")

    # --------------------------------------------------------
    # 첫 페이지 저장
    # --------------------------------------------------------

    save_raw_page(
        batch_id=batch_id,
        query_category_id=category_id,
        response_category_id=response_category_id,
        response_category_name=response_category_name,
        page=1,
        response_data=first_response,
    )

    print(f"[저장] 1/{last_page}")

    # --------------------------------------------------------
    # 2페이지 이후 동시 수집
    # --------------------------------------------------------

    pages = list(range(2, last_page + 1))

    responses = fetch_pages_concurrent(
        category_id=category_id,
        pages=pages,
        max_workers=max_workers,
    )

    # --------------------------------------------------------
    # 페이지 순서대로 DB 저장
    # --------------------------------------------------------

    for page in sorted(responses):
        response_data = responses[page]

        data = response_data.get(
            "data",
            {},
        )

        page_category_info = data.get("categoryInfo") or {}

        save_raw_page(
            batch_id=batch_id,
            query_category_id=category_id,
            response_category_id=(page_category_info.get("category_id")),
            response_category_name=(page_category_info.get("name")),
            page=page,
            response_data=response_data,
        )

        print(f"[저장] {page}/{last_page}")

    elapsed_time = time.perf_counter() - started_at

    print("[Extract 완료] " f"batch_id={batch_id}")

    print("[수집 시간] " f"{elapsed_time:.2f}초")

    return {
        "batch_id": batch_id,
        "query_category_id": category_id,
        "response_category_id": response_category_id,
        "response_category_name": response_category_name,
        "total_products": total_products,
        "total_pages": last_page,
        "collection_mode": "concurrent",
        "max_workers": max_workers,
        "elapsed_seconds": round(
            elapsed_time,
            2,
        ),
    }


# ============================================================
# 직접 실행
# ============================================================

if __name__ == "__main__":
    result = extract_category()

    print(result)
