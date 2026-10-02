-- ============================================================
-- MUJI Korea Data Analysis
-- ============================================================


-- 1. 일별 수집 상품 수
SELECT
    snapshot_date,
    COUNT(*) AS product_count
FROM muji_products
GROUP BY snapshot_date
ORDER BY snapshot_date;


-- 2. 상품 가격 분포 요약
SELECT
    snapshot_date,
    COUNT(*) AS product_count,
    ROUND(AVG(sell_price), 2) AS avg_price,
    MIN(sell_price) AS min_price,
    MAX(sell_price) AS max_price
FROM muji_products
GROUP BY snapshot_date
ORDER BY snapshot_date;


-- 3. 가격이 높은 상품
SELECT
    product_id,
    product_name,
    sell_price
FROM muji_products
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM muji_products
)
ORDER BY sell_price DESC
LIMIT 10;


-- 4. 할인 상품
SELECT
    product_id,
    product_name,
    retail_price,
    sell_price,
    discount_rate
FROM muji_products
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM muji_products
)
AND discount_rate > 0
ORDER BY discount_rate DESC;


-- 5. 리뷰가 많은 상품
SELECT
    product_id,
    product_name,
    review_count,
    review_score
FROM muji_products
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM muji_products
)
AND review_count > 0
ORDER BY review_count DESC
LIMIT 20;


-- 6. 재고 상태 집계
SELECT
    CASE
        WHEN stock = 0 THEN 'Out of Stock'
        WHEN stock BETWEEN 1 AND 5 THEN 'Low Stock (1-5)'
        ELSE 'Stock 6+'
    END AS stock_group,
    COUNT(*) AS option_count,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS rate
FROM muji_inventory
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM muji_inventory
)
GROUP BY
    CASE
        WHEN stock = 0 THEN 'Out of Stock'
        WHEN stock BETWEEN 1 AND 5 THEN 'Low Stock (1-5)'
        ELSE 'Stock 6+'
    END
ORDER BY option_count DESC;


-- 7. 상품별 총 관측 재고
SELECT
    i.product_id,
    p.product_name,
    COUNT(i.product_option_id) AS option_count,
    SUM(i.stock) AS total_stock
FROM muji_inventory AS i
JOIN muji_products AS p
    ON i.snapshot_date = p.snapshot_date
    AND i.product_id = p.product_id
WHERE i.snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM muji_inventory
)
GROUP BY
    i.product_id,
    p.product_name
ORDER BY total_stock ASC;


-- 8. 일별 상품별 관측 재고 변화
-- LAG()를 사용하여 이전 수집일과 비교한다.
-- stock_change는 판매량이 아닌 관측 재고 변화량이다.

WITH inventory_daily AS (
    SELECT
        snapshot_date,
        product_id,
        SUM(stock) AS total_stock
    FROM muji_inventory
    GROUP BY
        snapshot_date,
        product_id
),
inventory_change AS (
    SELECT
        snapshot_date,
        product_id,
        total_stock,
        LAG(total_stock) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS previous_stock
    FROM inventory_daily
)
SELECT
    snapshot_date,
    product_id,
    previous_stock,
    total_stock,
    total_stock - previous_stock AS stock_change
FROM inventory_change
WHERE previous_stock IS NOT NULL
ORDER BY ABS(stock_change) DESC;