from datetime import timedelta

import pendulum

from airflow.sdk import dag, task

from src.config import MUJI_CATEGORIES
from src.extract import extract_category
from src.transform import transform_batch

# ============================================================
# MUJI Korea Product Data Pipeline
#
# MUJI_CATEGORIES
#       ↓
# Extract (category)
#       ↓
# muji_raw
#       ↓
# Transform + Load
#       ↓
# muji_products / muji_inventory
#       ↓
# Validation
# ============================================================


@dag(
    dag_id="muji_korea_product_pipeline",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Seoul",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["project_03", "muji", "data-pipeline"],
)
def muji_korea_product_pipeline():

    # ========================================================
    # 1. Extract
    # ========================================================

    @task
    def extract_task(category_id: int) -> dict:
        """
        전달받은 MUJI 카테고리의 상품 데이터를 수집합니다.

        실제 JSON 데이터는 muji_raw에 직접 저장하고,
        XCom에는 batch_id 등 작은 메타데이터만 전달합니다.
        """

        result = extract_category(
            category_id=category_id,
        )

        print("======================================")
        print("[Extract Task 완료]")
        print(f"batch_id: {result['batch_id']}")
        print(f"요청 category_id: {result['query_category_id']}")
        print(f"요청 category_name: {result['query_category_name']}")
        print(f"응답 category_id: {result['response_category_id']}")
        print(f"응답 category_name: {result['response_category_name']}")
        print(f"상품 수: {result['total_products']:,}")
        print(f"페이지 수: {result['total_pages']:,}")
        print("======================================")

        return result

    # ========================================================
    # 2. Transform + Load
    # ========================================================

    @task
    def transform_task(extract_result: dict) -> dict:
        """
        Extract 단계에서 생성된 batch_id를 이용해
        raw 데이터를 정제하고 PostgreSQL에 적재합니다.

        Validation에 필요한 Extract 메타데이터도
        함께 반환합니다.
        """

        batch_id = extract_result["batch_id"]

        result = transform_batch(
            batch_id=batch_id,
        )

        # Validation에서 사용할 Extract 메타데이터
        result["expected_pages"] = extract_result["total_pages"]
        result["expected_products"] = extract_result["total_products"]
        result["query_category_name"] = extract_result["query_category_name"]

        print("======================================")
        print("[Transform Task 완료]")
        print(f"카테고리: {result['query_category_name']}")
        print(f"Raw 페이지: {result['raw_pages']:,}")
        print(f"상품 처리: {result['products_processed']:,}")
        print(f"옵션 처리: {result['inventory_processed']:,}")
        print("======================================")

        return result

    # ========================================================
    # 3. Validation
    # ========================================================

    @task
    def validate_task(transform_result: dict) -> None:
        """
        Extract 단계의 예상 수치와
        Transform 단계의 실제 처리 결과를 비교합니다.
        """

        expected_pages = transform_result["expected_pages"]
        expected_products = transform_result["expected_products"]

        raw_pages = transform_result["raw_pages"]
        products_processed = transform_result["products_processed"]
        inventory_processed = transform_result["inventory_processed"]

        category_name = transform_result["query_category_name"]

        if raw_pages != expected_pages:
            raise ValueError(
                f"[{category_name}] 페이지 수 불일치: "
                f"expected={expected_pages}, "
                f"actual={raw_pages}"
            )

        if products_processed <= 0:
            raise ValueError(f"[{category_name}] 처리된 상품이 없습니다.")

        if products_processed != expected_products:
            raise ValueError(
                f"[{category_name}] 상품 수 불일치: "
                f"expected={expected_products}, "
                f"actual={products_processed}"
            )

        print("======================================")
        print("[Validation 성공]")
        print(f"카테고리: {category_name}")
        print(f"페이지: {raw_pages:,}")
        print(f"상품: {products_processed:,}")
        print(f"옵션: {inventory_processed:,}")
        print("======================================")

    # ========================================================
    # DAG FLOW
    # ========================================================

    category_ids = list(MUJI_CATEGORIES.keys())

    # 4개 카테고리 → Extract 4개
    extract_results = extract_task.expand(
        category_id=category_ids,
    )

    # 각 Extract 결과 → Transform 1:1 매핑
    transform_results = transform_task.expand(
        extract_result=extract_results,
    )

    # 각 Transform 결과 → Validation 1:1 매핑
    validate_task.expand(
        transform_result=transform_results,
    )


muji_korea_product_pipeline()
