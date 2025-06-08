cs2

### SQL
```sql
CREATE VIEW test AS
SELECT * FROM read_parquet('parquet/*');

PRAGMA set_max_column_width=0;
PRAGMA set_max_rows=1000000;

```

## SQL

```sql
create table test as select * from read_parquet('parquet/*');
```

buff 最近 n 个时间节点，价格增长最快的 n 件饰品
```sql
WITH recent_times AS (
    SELECT DISTINCT updateTime
    FROM test
    ORDER BY updateTime DESC
    LIMIT 2
    ),
    latest_prices AS (
SELECT
    goodId,
    name,
    updateTime,
    yyypSellPrice as sell_price,
    ROW_NUMBER() OVER (PARTITION BY goodId ORDER BY updateTime DESC) AS rn
FROM test
WHERE updateTime IN (SELECT updateTime FROM recent_times)
    ),
    pivoted AS (
SELECT
    goodId,
    name,
    MAX(CASE WHEN rn = 1 THEN sell_price END) AS latest_price,
    MAX(CASE WHEN rn = 2 THEN sell_price END) AS prev_price
FROM latest_prices
GROUP BY goodId,name
    )
SELECT
    goodId,
    name,
    latest_price,
    prev_price,
    (latest_price - prev_price) AS price_increase
FROM pivoted
WHERE latest_price IS NOT NULL AND prev_price IS NOT NULL
ORDER BY price_increase DESC
    LIMIT 10;

```

库存增加最多的商品
```sql
WITH recent_times AS (
    SELECT DISTINCT updateTime
    FROM test
    ORDER BY updateTime DESC
    LIMIT 3
),
latest_num AS (
    SELECT
        goodId,
        name,
        updateTime,
        (buffSellNum + yyypSellNum + steamSellNum) as total_num,
        ROW_NUMBER() OVER (PARTITION BY goodId ORDER BY updateTime DESC) AS rn
    FROM test
    WHERE updateTime IN (SELECT updateTime FROM recent_times)
),
pivoted AS (
    SELECT
        goodId,
        name,
        MAX(CASE WHEN rn = 1 THEN total_num END) AS latest_total,
        MAX(CASE WHEN rn = 2 THEN total_num END) AS prev_total
    FROM latest_num
    GROUP BY goodId,name
)
SELECT
    goodId,
    name,
    prev_total,
    latest_total,
    (latest_total - prev_total) AS total_num_increase
FROM pivoted
WHERE latest_total IS NOT NULL AND prev_total IS NOT NULL
ORDER BY total_num_increase DESC
LIMIT 10;

```

库存减少最多的商品
```sql
WITH recent_times AS (
    SELECT DISTINCT updateTime
    FROM test
    ORDER BY updateTime DESC
    LIMIT 3
    ),
    latest_num AS (
SELECT
    goodId,
    name,
    buffSellPrice,
    yyypSellPrice,
    updateTime,
    (buffSellNum + yyypSellNum + steamSellNum) as total_num,
    ROW_NUMBER() OVER (PARTITION BY goodId ORDER BY updateTime DESC) AS rn
FROM test
WHERE updateTime IN (SELECT updateTime FROM recent_times)
    ),
    pivoted AS (
SELECT
    goodId,
    name,
    MAX(CASE WHEN rn = 1 THEN buffSellPrice END) as buffSellPrice,
    MAX(CASE WHEN rn = 1 THEN yyypSellPrice END) as yyypSellPrice,
    MAX(CASE WHEN rn = 1 THEN total_num END) AS latest_total,
    MAX(CASE WHEN rn = 2 THEN total_num END) AS prev_total
FROM latest_num
GROUP BY goodId,name
    )
SELECT
    goodId,
    name,
    buffSellPrice,
    yyypSellPrice,
    prev_total,
    latest_total,
    (prev_total - latest_total) AS total_num_decrease
FROM pivoted
WHERE latest_total IS NOT NULL AND prev_total IS NOT NULL AND yyypSellPrice > 500
ORDER BY total_num_decrease DESC
    LIMIT 50;


```