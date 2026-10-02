# MUJI Korea Product Data Pipeline

MUJI Korea 온라인 스토어의 상품 데이터를 수집하고,
PostgreSQL에 저장한 뒤 Apache Airflow로 자동화한 데이터 파이프라인 프로젝트입니다.

단순 API 수집에서 끝나지 않고

**수집 → Raw 저장 → 정제 → 적재 → 검증 → 분석**

과정을 하나의 파이프라인으로 구성했습니다.

---

## 1. Overview

MUJI Korea의 공개 상품 API에서 다음 4개 카테고리를 수집합니다.

| Category | Products |
|---|---:|
| 의복 | 1,034 |
| 생활 | 2,127 |
| 식품 | 152 |
| 뷰티 | 320 |
| **Total** | **3,633** |

최신 스냅샷 기준:

- 상품 **3,633개**
- 상품 옵션 **14,601개**
- 상품 ID 중복 **0개**
- 옵션 ID 중복 **0개**

> API 데이터는 변경될 수 있으므로 상품 수는 수집 시점에 따라 달라질 수 있습니다.

---

## 2. Pipeline

```text
MUJI Korea API
       │
       ▼
    Extract
(ThreadPoolExecutor)
 Retry / Backoff
       │
       ▼
   PostgreSQL
    muji_raw
       │
       ▼
   Transform
  JSON → Table
     UPSERT
       │
       ▼
   PostgreSQL
 muji_products
 muji_inventory
       │
       ▼
    Validate
       │
       ▼
Jupyter Notebook
 EDA / SQL / Chart
```

Airflow에서는 카테고리별 작업을 Dynamic Task Mapping으로 실행합니다.

```text
extract_task × 4
       ↓
transform_task × 4
       ↓
validate_task × 4
```

---

## 3. Data Collection

### Concurrent Requests

MUJI API는 상품 목록을 페이지 단위로 반환합니다.

각 페이지 요청이 독립적이라는 점을 이용하여
`ThreadPoolExecutor`로 여러 페이지를 동시에 수집했습니다.

10개 페이지를 대상으로 순차/동시 수집 시간을 비교한 결과:

| Method | Time |
|---|---:|
| Sequential | 4.85 sec |
| Concurrent | 0.62 sec |
| **Speedup** | **7.84x** |

API 요청 실패에 대응하기 위해 Retry와 Exponential Backoff도 적용했습니다.

```python
delay = 2 ** attempt
```

상품과 옵션에는 각각 `product_id`, `product_option_id`가 존재하며,
가격·할인·리뷰·재고처럼 집계 가능한 수치 데이터도 함께 수집합니다.

---

## 4. Storage & Transform

API 원본과 분석용 데이터를 분리하여 저장합니다.

```text
muji_raw
└── API 페이지별 Raw JSON

muji_products
└── 상품 / 가격 / 할인 / 리뷰

muji_inventory
└── 상품 옵션 / 색상 / 사이즈 / 재고
```

Raw JSON을 보존하기 때문에 Transform 로직이 변경되더라도
API를 다시 호출하지 않고 기존 데이터를 재처리할 수 있습니다.

정제 데이터는 PostgreSQL의 Primary Key와 UPSERT를 이용하여 저장합니다.

### Idempotency

생활 카테고리의 동일 `batch_id`를 대상으로 Transform을 다시 실행하여
중복 데이터가 생성되는지 확인했습니다.

```text
                Before    After
Products         2,126    2,126
Inventory        2,117    2,117
```

재실행 전후 행 수가 동일하게 유지되어
동일 batch 재처리 시 중복 행이 추가되지 않는 것을 확인했습니다.

---

## 5. Data Quality

분석 전 상품·옵션 ID의 중복과 주요 컬럼의 결측치를 확인했습니다.

재고 관련 필드는 다음과 같은 차이가 있었습니다.

| Metric | Missing | Negative |
|---|---:|---:|
| `stock` | 0 | 0 |
| `muji_kr_inventory` | 10,779 | 0 |
| `muji_dc_stock` | 2,235 | 633 |

`stock`은 결측치와 음수 값이 없어 주요 관측 재고 지표로 사용했습니다.

반면 `muji_kr_inventory`와 `muji_dc_stock`은
결측 또는 음수 값이 존재하여 핵심 재고 분석에서는 제외했습니다.

---

## 6. Analysis

분석은 PostgreSQL에 저장된 실제 최신 스냅샷을 사용합니다.

```text
notebooks/muji_analysis.ipynb
```

### Price

| Category | Average Price | Median Price |
|---|---:|---:|
| 의복 | 46,968 | 29,900 |
| 생활 | 51,690 | 9,900 |
| 식품 | 4,549 | 3,900 |
| 뷰티 | 7,395 | 4,500 |

생활 카테고리는 평균과 중앙값의 차이가 크게 나타났습니다.
이는 일부 고가 상품의 영향으로 가격 분포가 오른쪽으로 치우쳐 있음을 시사합니다.

### Discount

의복은 전체 상품의 **12.28%**가 할인 중이었으며,
할인 상품의 평균 할인율은 **42.05%**였습니다.

생활, 식품, 뷰티의 할인 상품 비율은 모두 3% 미만이었습니다.

### Review

전체 3,633개 상품 중 3,200개에는 리뷰가 존재했고,
433개에는 리뷰가 없었습니다.

리뷰가 없는 433개 상품은 모두 `review_score=0`으로 나타나
본 데이터에서 평점 0은 리뷰 미등록 상태를 나타내는 값으로 판단했습니다.

### Inventory

| Category | Out of Stock | Low Stock (1~5) | Median Stock |
|---|---:|---:|---:|
| 의복 | 43.56% | 27.99% | 2 |
| 생활 | 11.99% | 1.51% | 71 |
| 뷰티 | 2.19% | 0.63% | 151.5 |

의복은 관측 시점 기준 품절 및 저재고 옵션 비율이
다른 카테고리보다 높게 나타났습니다.

다만 `stock`은 특정 시점의 관측값이므로
이를 실제 판매량이나 상품 인기도로 해석하지 않습니다.

식품은 API에서 옵션 데이터가 제공되지 않아 재고 분석에서 제외했습니다.

---

## 7. SQL Analysis

분석 과정에서 `COUNT`, `AVG`, `FILTER`, `PERCENTILE_CONT` 등의
집계 함수와 Window Function을 사용했습니다.

카테고리별 가격 순위:

```sql
RANK() OVER (
    PARTITION BY query_category_id
    ORDER BY sell_price DESC
)
```

상품별 이전 관측 재고:

```sql
LAG(total_stock) OVER (
    PARTITION BY product_id
    ORDER BY snapshot_date
)
```

현재는 하나의 날짜에 대한 재고 스냅샷만 존재하므로
`LAG()`로 비교할 이전 데이터가 없습니다.

Airflow를 통해 일별 데이터가 누적되면
동일한 쿼리로 상품별 관측 재고 변화를 분석할 수 있습니다.

Notebook에는 실제 수집 데이터를 기반으로 한
가격 및 재고 시각화와 실행 결과를 함께 저장했습니다.

---

## 8. Project Structure

```text
project_03/
├── dags/
│   └── muji_pipeline.py
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── db.py
│   ├── extract.py
│   └── transform.py
├── sql/
│   ├── init.sql
│   ├── schema.sql
│   └── analysis.sql
├── notebooks/
│   └── muji_analysis.ipynb
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

## 9. Tech Stack

- Python
- PostgreSQL 16
- Apache Airflow 3
- Docker / Docker Compose
- Pandas
- Matplotlib
- Psycopg
- Jupyter Notebook

---

## 10. Run

프로젝트 루트에 `.env` 파일을 생성합니다.

```dotenv
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=muji
POSTGRES_DSN=postgresql://postgres:postgres@postgres:5432/muji

AIRFLOW_JWT_SECRET=<your-secret>
```

`.env`는 Git에 포함하지 않습니다.

Docker 환경을 실행합니다.

```bash
docker compose up -d
```

Airflow에서 다음 DAG를 실행합니다.

```text
muji_korea_product_pipeline
```

분석 결과는 다음 Notebook에서 확인할 수 있습니다.

```text
notebooks/muji_analysis.ipynb
```

---

## 11. Limitations

- 데이터는 MUJI Korea 온라인 스토어 API의 특정 시점 스냅샷입니다.
- 관측 재고 감소를 실제 판매량으로 해석할 수 없습니다.
- 식품은 옵션 데이터가 제공되지 않아 재고 분석에서 제외했습니다.
- 현재 시계열 데이터가 하루치이므로 장기적인 재고 변화 분석에는 한계가 있습니다.
- API 데이터 변경에 따라 상품 수와 분석 결과가 달라질 수 있습니다.