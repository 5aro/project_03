# MUJI Korea Data Pipeline

MUJI Korea 온라인 스토어의 생활 카테고리 상품 데이터를 수집하고,
PostgreSQL에 저장한 뒤 Airflow로 자동화하는 데이터 파이프라인 프로젝트입니다.

상품 가격, 리뷰, 옵션별 재고 데이터를 일별 스냅샷으로 저장하여
향후 가격 및 재고 변화를 분석할 수 있도록 구성했습니다.

---

## 1. Project Overview

### 목적

- 공개 API 기반 상품 데이터 수집
- 동시 요청을 통한 수집 성능 개선
- Raw / Processed 데이터 분리 저장
- PostgreSQL 기반 일별 Snapshot 관리
- Airflow를 이용한 ETL 파이프라인 자동화
- Pandas / SQL 기반 데이터 품질 및 분포 분석
- Window Function을 활용한 일별 변화 분석

### 수집 대상

- Source: MUJI Korea Online Store
- Category: 생활
- 수집 데이터
  - 상품 ID / 상품명
  - 정상가 / 판매가 / 할인율
  - 리뷰 수 / 리뷰 평점
  - 판매 상태
  - 상품 옵션
  - 옵션별 관측 재고

> 재고 데이터는 API에서 수집한 시점의 관측값이며,
> 재고 감소를 실제 판매량으로 해석하지 않습니다.

---

## 2. Architecture

```text
MUJI Korea API
      │
      ▼
Airflow Extract Task
      │
      ├── ThreadPoolExecutor
      │
      ▼
PostgreSQL
  └── muji_raw
      │
      ▼
Airflow Transform Task
      │
      ├── Data Cleaning
      └── UPSERT
      │
      ▼
PostgreSQL
  ├── muji_products
  └── muji_inventory
      │
      ▼
Airflow Validate Task
      │
      ▼
SQL / Pandas Analysis
```

---

## 3. Tech Stack

- Python
- PostgreSQL 16
- Apache Airflow 3
- Docker / Docker Compose
- Pandas
- Matplotlib
- psycopg
- requests
- ThreadPoolExecutor

---

## 4. Project Structure

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

## 5. Data Pipeline

### Extract

MUJI Korea API의 pagination을 확인한 뒤 전체 페이지를 수집합니다.

`ThreadPoolExecutor`를 사용하여 여러 페이지를 동시에 요청하며,
요청 실패 시 exponential backoff 방식으로 재시도합니다.

```text
API
 ↓
Page 1 요청
 ↓
전체 페이지 수 확인
 ↓
Page 2 ~ N 동시 요청
 ↓
Raw JSON 저장
```

### Transform

수집된 Raw JSON을 상품과 옵션 데이터로 분리합니다.

```text
muji_raw
   │
   ├── Product
   │      ↓
   │  muji_products
   │
   └── Options
          ↓
      muji_inventory
```

### Validate

수집한 페이지 수와 상품 및 옵션 수를 확인하여
파이프라인 실행 결과를 검증합니다.

---

## 6. Database

### `muji_raw`

API 응답 원본 JSON을 저장합니다.

주요 컬럼:

- `batch_id`
- `query_category_id`
- `page`
- `collected_at`
- `raw_json`
- `content_hash`

### `muji_products`

일별 상품 Snapshot을 저장합니다.

Primary Key:

```text
(snapshot_date, product_id)
```

### `muji_inventory`

일별 상품 옵션 재고 Snapshot을 저장합니다.

Primary Key:

```text
(snapshot_date, product_option_id)
```

---

## 7. Concurrency Performance

동일한 10개 페이지를 대상으로 순차 수집과 동시 수집 성능을 비교했습니다.

| Method | Time |
|---|---:|
| Sequential | 4.85 sec |
| Concurrent | 0.62 sec |

```text
Speedup: 7.84x
Workers: 5
```

동일한 API 요청 범위에서 `ThreadPoolExecutor`를 적용했을 때
테스트 환경 기준 약 7.84배 빠른 수집 시간을 확인했습니다.

---

## 8. Idempotency

동일한 `batch_id`에 대해 Transform 작업을 다시 실행하여
중복 데이터가 생성되는지 확인했습니다.

재실행 후:

```text
muji_products
total rows   : 2,126
unique rows  : 2,126

muji_inventory
total rows   : 2,117
unique rows  : 2,117
```

Primary Key와 PostgreSQL UPSERT를 사용하여
동일 Snapshot의 파이프라인 재실행 시 중복 데이터가 증가하지 않도록 구성했습니다.

---

## 9. Data Analysis

최신 Snapshot 기준:

```text
Products : 2,126
Options  : 2,117
```

### Price

- 평균 판매가격: 약 51,536원
- 중앙값: 9,900원
- 최대값: 999,000원

평균과 중앙값의 차이가 크며 일부 고가 상품으로 인해
오른쪽 꼬리가 긴 가격 분포가 관찰되었습니다.

### Reviews

`review_count=0`, `review_score=0`인 상품이 274개 확인되었습니다.

따라서 리뷰 평점 분석에서는 리뷰가 존재하는 상품과
리뷰가 없는 상품을 구분하여 분석했습니다.

### Inventory

전체 2,117개 옵션 기준:

| Stock Status | Options | Rate |
|---|---:|---:|
| Out of Stock | 252 | 11.90% |
| Low Stock (1-5) | 33 | 1.56% |
| Stock 6+ | 1,832 | 86.54% |

`stock`에는 결측값이 없었으며 재고 분석의 주요 지표로 사용했습니다.

---

## 10. Window Function

일별 Snapshot이 누적되면 상품별 관측 재고 변화를 비교할 수 있도록
PostgreSQL의 `LAG()` Window Function을 사용했습니다.

```sql
LAG(total_stock) OVER (
    PARTITION BY product_id
    ORDER BY snapshot_date
)
```

현재 관측값과 이전 수집일의 값을 비교하여:

```text
stock_change = current_stock - previous_stock
```

을 계산합니다.

재고 감소에는 판매뿐 아니라 재입고, 재고 조정, 데이터 변경 등
여러 요인이 영향을 줄 수 있으므로 판매량으로 해석하지 않습니다.

---

## 11. Notebook

`notebooks/muji_analysis.ipynb`에서 실제 수집 데이터를 이용해 다음을 분석합니다.

- 결측치
- 수치형 데이터 분포
- 가격 분포
- 리뷰 데이터
- 옵션별 재고
- 상품별 총 관측 재고
- 재고 상태
- 일별 재고 변화

Notebook에는 코드뿐 아니라 실행 결과와 시각화 결과도 함께 보존합니다.

---

## 12. Limitations

현재 데이터는 MUJI Korea 온라인 스토어 API에서 관측한 데이터입니다.

따라서 다음과 같은 한계가 있습니다.

- 실제 판매량 데이터가 아님
- 재고 변화 원인을 직접 확인할 수 없음
- API 필드의 내부 비즈니스 정의를 모두 알 수 없음
- 충분한 시계열 분석을 위해서는 일별 Snapshot 누적이 필요함

향후 데이터를 지속적으로 수집하여 상품별 가격 및 재고 변화 추이를
분석할 수 있도록 확장할 예정입니다.