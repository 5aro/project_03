from datetime import timedelta

import pendulum

from airflow.sdk import dag, task

from src.extract import extract_category
from src.transform import transform_batch

# ============================================================
# MUJI Korea Product Data Pipeline
#
# MUJI API
#     ↓
# Extract
#     ↓
# muji_raw
#     ↓
# Transform + Load
#     ↓
# muji_products / muji_inventory
#     ↓
# Validation
# ============================================================


@dag(
    dag_id="muji_korea_product_pipeline",

    # 매일 한국시간 오전 9시 실행
    schedule="0 9 * * *",

    # DAG 기준 시간대를 Asia/Seoul로 지정
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
    def extract_task() -> dict:
        """
        MUJI Korea API에서 상품 데이터를 수집합니다.

        실제 JSON 데이터는 muji_raw에 직접 저장하고,
        XCom에는 batch_id 등 작은 메타데이터만 전달합니다.
        """

        result = extract_category()

        print("======================================")

        print("[Extract Task 완료]")

        print(f"batch_id: " f"{result['batch_id']}")

        print(f"요청 category_id: " f"{result['query_category_id']}")

        print(f"응답 category_id: " f"{result['response_category_id']}")

        print(f"응답 category_name: " f"{result['response_category_name']}")

        print(f"상품 수: " f"{result['total_products']:,}")

        print(f"페이지 수: " f"{result['total_pages']:,}")

        print("======================================")

        return result

    # ========================================================
    # 2. Transform + Load
    # ========================================================

    @task
    def transform_task(
        extract_result: dict,
    ) -> dict:
        """
        Extract 단계에서 생성된 batch_id를 이용해
        muji_raw 데이터를 읽습니다.

        이후 데이터를 정제하여

        - muji_products
        - muji_inventory

        테이블에 저장합니다.
        """

        batch_id = extract_result["batch_id"]

        result = transform_batch(batch_id=batch_id)

        print("======================================")

        print("[Transform Task 완료]")

        print(f"batch_id: " f"{result['batch_id']}")

        print(f"snapshot_date: " f"{result['snapshot_date']}")

        print(f"Raw 페이지: " f"{result['raw_pages']:,}")

        print(f"처리 상품: " f"{result['products_processed']:,}")

        print(f"처리 옵션: " f"{result['inventory_processed']:,}")

        print("======================================")

        return result

    # ========================================================
    # 3. Validation
    # ========================================================

    @task
    def validate_task(
        extract_result: dict,
        transform_result: dict,
    ) -> None:
        """
        파이프라인 실행 결과를 간단하게 검증합니다.

        현재 프로젝트에서는 별도의 데이터 품질 도구를
        사용하지 않고 기본적인 건수 검증만 수행합니다.
        """

        expected_pages = extract_result["total_pages"]

        expected_products = extract_result["total_products"]

        raw_pages = transform_result["raw_pages"]

        products_processed = transform_result["products_processed"]

        inventory_processed = transform_result["inventory_processed"]

        # ----------------------------------------------------
        # Raw 페이지 검증
        # ----------------------------------------------------

        if raw_pages != expected_pages:
            raise ValueError(
                "Raw 페이지 수가 일치하지 않습니다. "
                f"API={expected_pages}, "
                f"DB={raw_pages}"
            )

        # ----------------------------------------------------
        # 상품 수 검증
        # ----------------------------------------------------

        if products_processed <= 0:
            raise ValueError("처리된 상품이 없습니다.")

        if products_processed != expected_products:
            raise ValueError(
                "상품 수가 일치하지 않습니다. "
                f"API={expected_products}, "
                f"Transform={products_processed}"
            )

        # ----------------------------------------------------
        # 옵션 데이터 검증
        # ----------------------------------------------------

        if inventory_processed <= 0:
            raise ValueError("처리된 상품 옵션이 없습니다.")

        # ----------------------------------------------------
        # 성공
        # ----------------------------------------------------

        print("======================================")

        print("[Validation 성공]")

        print(f"Raw 페이지: " f"{raw_pages:,}")

        print(f"상품: " f"{products_processed:,}")

        print(f"옵션: " f"{inventory_processed:,}")

        print("======================================")

    # ========================================================
    # DAG FLOW
    # ========================================================

    extract_result = extract_task()

    transform_result = transform_task(extract_result)

    validate_task(
        extract_result,
        transform_result,
    )


muji_korea_product_pipeline()
