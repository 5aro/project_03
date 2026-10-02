-- ============================================================
-- MUJI Korea Data Pipeline
-- PostgreSQL Schema
-- ============================================================


-- ============================================================
-- 1. RAW
-- API 응답 원본을 페이지 단위로 저장
-- ============================================================

CREATE TABLE IF NOT EXISTS muji_raw (
    id BIGSERIAL PRIMARY KEY,

    batch_id UUID NOT NULL,

    -- 실제 API 요청에 사용한 category_id
    query_category_id INTEGER NOT NULL,

    -- API 응답 categoryInfo
    response_category_id INTEGER,
    response_category_name TEXT,

    page INTEGER NOT NULL,

    collected_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    raw_json JSONB NOT NULL,

    content_hash TEXT NOT NULL,

    UNIQUE (
        batch_id,
        query_category_id,
        page
    )
);


CREATE INDEX IF NOT EXISTS idx_muji_raw_batch
ON muji_raw (batch_id);


-- ============================================================
-- 2. PRODUCT SNAPSHOT
-- 상품 단위 일별 데이터
-- ============================================================

CREATE TABLE IF NOT EXISTS muji_products (
    snapshot_date DATE NOT NULL,

    batch_id UUID NOT NULL,

    query_category_id INTEGER NOT NULL,

    response_category_id INTEGER,
    response_category_name TEXT,

    product_id BIGINT NOT NULL,
    product_name TEXT NOT NULL,

    retail_price INTEGER,
    sell_price INTEGER,
    last_price INTEGER,

    discount_rate NUMERIC,

    review_count INTEGER,
    review_score NUMERIC,

    sale_state TEXT,

    is_restock_expected BOOLEAN,

    collected_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        snapshot_date,
        product_id
    )
);


CREATE INDEX IF NOT EXISTS idx_muji_products_product
ON muji_products (product_id);


CREATE INDEX IF NOT EXISTS idx_muji_products_date
ON muji_products (snapshot_date);


-- ============================================================
-- 3. INVENTORY / OPTION SNAPSHOT
-- 옵션 단위 일별 데이터
-- ============================================================

CREATE TABLE IF NOT EXISTS muji_inventory (
    snapshot_date DATE NOT NULL,

    batch_id UUID NOT NULL,

    product_id BIGINT NOT NULL,

    product_option_id BIGINT NOT NULL,

    color_name TEXT,
    size TEXT,

    -- API 원본 재고 관련 지표
    -- 실제 판매량으로 해석하지 않음
    stock INTEGER,
    muji_kr_inventory INTEGER,
    muji_dc_stock INTEGER,

    sale_state TEXT,

    is_display INTEGER,

    option_barcode TEXT,

    muji_release_date TIMESTAMP,

    collected_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        snapshot_date,
        product_option_id
    )
);


CREATE INDEX IF NOT EXISTS idx_muji_inventory_product
ON muji_inventory (product_id);


CREATE INDEX IF NOT EXISTS idx_muji_inventory_option
ON muji_inventory (product_option_id);


CREATE INDEX IF NOT EXISTS idx_muji_inventory_date
ON muji_inventory (snapshot_date);