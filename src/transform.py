from datetime import datetime
from zoneinfo import ZoneInfo

from src.db import get_connection

# ============================================================
# 1. Raw 데이터 조회
# ============================================================


def get_raw_pages(
    batch_id: str,
) -> list[dict]:
    """
    특정 batch_id에 해당하는 Raw 데이터를
    PostgreSQL에서 페이지 순서대로 가져옵니다.

    API를 다시 호출하지 않습니다.
    """

    sql = """
        SELECT
            query_category_id,
            response_category_id,
            response_category_name,
            raw_json
        FROM muji_raw
        WHERE batch_id = %s
        ORDER BY page
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (batch_id,),
            )

            rows = cur.fetchall()

    return [
        {
            "query_category_id": row[0],
            "response_category_id": row[1],
            "response_category_name": row[2],
            "raw_json": row[3],
        }
        for row in rows
    ]


# ============================================================
# 2. 상품 Transform
# ============================================================


def transform_product(
    product: dict,
    batch_id: str,
    query_category_id: int,
    response_category_id: int | None,
    response_category_name: str | None,
    snapshot_date,
) -> tuple:
    """
    MUJI 상품 JSON 하나를
    muji_products 테이블에 저장할 형태로 변환합니다.
    """

    return (
        snapshot_date,
        batch_id,
        query_category_id,
        response_category_id,
        response_category_name,
        product.get("product_id"),
        product.get("product_name"),
        product.get("retail_price"),
        product.get("sell_price"),
        product.get("last_price"),
        product.get("discount_rate"),
        product.get("review_count"),
        product.get("review_score"),
        product.get("sale_state"),
        product.get("is_restock_expected"),
    )


# ============================================================
# 3. Inventory / Option Transform
# ============================================================


def transform_inventory(
    product: dict,
    batch_id: str,
    snapshot_date,
) -> list[tuple]:
    """
    상품 하나에 포함된 옵션 데이터를
    옵션 단위 행으로 변환합니다.

    예:

    티셔츠
        검정 / S
        검정 / M
        검정 / L
        흰색 / S
        흰색 / M

    → 각각 별도 행으로 변환
    """

    result = []

    product_id = product.get("product_id")

    options = product.get("options") or {}

    # ========================================================
    # 색상 옵션 반복
    # ========================================================

    for (
        color_key,
        color_option,
    ) in options.items():

        sizes = color_option.get("sizes") or []

        # ====================================================
        # 사이즈 옵션 반복
        # ====================================================

        for option in sizes:

            product_option_id = option.get("product_option_id")

            # 옵션 ID가 없으면
            # PK 생성이 불가능하므로 제외
            if product_option_id is None:
                continue

            color_name = (
                option.get("color_name") or color_option.get("color_name") or color_key
            )

            result.append(
                (
                    snapshot_date,
                    batch_id,
                    product_id,
                    product_option_id,
                    color_name,
                    option.get("size"),
                    option.get("stock"),
                    option.get("muji_kr_inventory"),
                    option.get("muji_dc_stock"),
                    option.get("sale_state"),
                    option.get("is_display"),
                    option.get("option_barcode"),
                    option.get("muji_release_date"),
                )
            )

    return result


# ============================================================
# 4. Product Load
# ============================================================


def load_product(
    cur,
    product_row: tuple,
):
    """
    상품 데이터를 muji_products에 저장합니다.

    같은 날짜 + 같은 상품이 이미 존재하면
    새로운 값으로 UPDATE합니다.
    """

    sql = """
        INSERT INTO muji_products (
            snapshot_date,
            batch_id,
            query_category_id,
            response_category_id,
            response_category_name,
            product_id,
            product_name,
            retail_price,
            sell_price,
            last_price,
            discount_rate,
            review_count,
            review_score,
            sale_state,
            is_restock_expected
        )

        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )

        ON CONFLICT (
            snapshot_date,
            product_id
        )

        DO UPDATE SET

            batch_id =
                EXCLUDED.batch_id,

            query_category_id =
                EXCLUDED.query_category_id,

            response_category_id =
                EXCLUDED.response_category_id,

            response_category_name =
                EXCLUDED.response_category_name,

            product_name =
                EXCLUDED.product_name,

            retail_price =
                EXCLUDED.retail_price,

            sell_price =
                EXCLUDED.sell_price,

            last_price =
                EXCLUDED.last_price,

            discount_rate =
                EXCLUDED.discount_rate,

            review_count =
                EXCLUDED.review_count,

            review_score =
                EXCLUDED.review_score,

            sale_state =
                EXCLUDED.sale_state,

            is_restock_expected =
                EXCLUDED.is_restock_expected,

            collected_at =
                NOW()
    """

    cur.execute(
        sql,
        product_row,
    )


# ============================================================
# 5. Inventory Load
# ============================================================


def load_inventory(
    cur,
    inventory_row: tuple,
):
    """
    옵션 데이터를 muji_inventory에 저장합니다.

    같은 날짜 + 같은 옵션이 이미 존재하면
    새로운 값으로 UPDATE합니다.
    """

    sql = """
        INSERT INTO muji_inventory (
            snapshot_date,
            batch_id,
            product_id,
            product_option_id,
            color_name,
            size,
            stock,
            muji_kr_inventory,
            muji_dc_stock,
            sale_state,
            is_display,
            option_barcode,
            muji_release_date
        )

        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )

        ON CONFLICT (
            snapshot_date,
            product_option_id
        )

        DO UPDATE SET

            batch_id =
                EXCLUDED.batch_id,

            product_id =
                EXCLUDED.product_id,

            color_name =
                EXCLUDED.color_name,

            size =
                EXCLUDED.size,

            stock =
                EXCLUDED.stock,

            muji_kr_inventory =
                EXCLUDED.muji_kr_inventory,

            muji_dc_stock =
                EXCLUDED.muji_dc_stock,

            sale_state =
                EXCLUDED.sale_state,

            is_display =
                EXCLUDED.is_display,

            option_barcode =
                EXCLUDED.option_barcode,

            muji_release_date =
                EXCLUDED.muji_release_date,

            collected_at =
                NOW()
    """

    cur.execute(
        sql,
        inventory_row,
    )


# ============================================================
# 6. Batch Transform
# ============================================================


def transform_batch(
    batch_id: str,
) -> dict:
    """
    특정 batch의 Raw 데이터를 읽어

    muji_products
    muji_inventory

    두 테이블로 변환/적재합니다.
    """

    print("[Transform 시작] " f"batch_id={batch_id}")

    # ========================================================
    # Raw 조회
    # ========================================================

    raw_pages = get_raw_pages(batch_id)

    if not raw_pages:
        raise RuntimeError("Raw 데이터가 없습니다. " f"batch_id={batch_id}")

    # ========================================================
    # Snapshot 날짜
    # ========================================================

    snapshot_date = datetime.now(ZoneInfo("Asia/Seoul")).date()

    product_count = 0
    inventory_count = 0

    # ========================================================
    # PostgreSQL 연결
    # ========================================================

    with get_connection() as conn:

        with conn.cursor() as cur:

            # =================================================
            # 페이지 반복
            # =================================================

            for raw_page in raw_pages:

                query_category_id = raw_page["query_category_id"]

                response_category_id = raw_page["response_category_id"]

                response_category_name = raw_page["response_category_name"]

                raw_response = raw_page["raw_json"]

                # =============================================
                # API data
                # =============================================

                data = raw_response.get("data") or {}

                products = data.get("rows") or []

                # =============================================
                # 상품 반복
                # =============================================

                for product in products:

                    product_id = product.get("product_id")

                    product_name = product.get("product_name")

                    # 필수 데이터가 없으면 제외
                    if product_id is None:
                        continue

                    if not product_name:
                        continue

                    # =========================================
                    # Product Transform
                    # =========================================

                    product_row = transform_product(
                        product=product,
                        batch_id=(batch_id),
                        query_category_id=(query_category_id),
                        response_category_id=(response_category_id),
                        response_category_name=(response_category_name),
                        snapshot_date=(snapshot_date),
                    )

                    # =========================================
                    # Product Load
                    # =========================================

                    load_product(
                        cur,
                        product_row,
                    )

                    product_count += 1

                    # =========================================
                    # Inventory Transform
                    # =========================================

                    inventory_rows = transform_inventory(
                        product=product,
                        batch_id=(batch_id),
                        snapshot_date=(snapshot_date),
                    )

                    # =========================================
                    # Inventory Load
                    # =========================================

                    for inventory_row in inventory_rows:

                        load_inventory(
                            cur,
                            inventory_row,
                        )

                        inventory_count += 1

        # ====================================================
        # 모든 데이터 처리 후 Commit
        # ====================================================

        conn.commit()

    # ========================================================
    # 결과
    # ========================================================

    result = {
        "batch_id": batch_id,
        "snapshot_date": str(snapshot_date),
        "raw_pages": len(raw_pages),
        "products_processed": product_count,
        "inventory_processed": inventory_count,
    }

    print("[Transform 완료]")

    print(f"Raw 페이지: " f"{len(raw_pages):,}")

    print(f"상품: " f"{product_count:,}")

    print(f"옵션: " f"{inventory_count:,}")

    return result


# ============================================================
# 7. 직접 실행 테스트
# ============================================================

if __name__ == "__main__":

    # extract.py를 먼저 실행한 후
    # 출력된 batch_id를 입력합니다.

    test_batch_id = "여기에-batch-id-입력"

    result = transform_batch(test_batch_id)

    print(result)
