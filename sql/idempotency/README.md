# Idempotency (Gold Publish)

## Context
During testing, re-running the Gold publish can insert rows that were already present, producing duplicates in `gold.fact_sales`.
This affects `COUNT(*)`, while `COUNT(DISTINCT invoice_line_no)` (business metric) stays stable.

## Metrics
```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT invoice_line_no) AS distinct_invoice_lines
FROM gold.fact_sales;
```

## Duplicate check
```sql
SELECT
  invoice_line_no,
  COUNT(*) AS occurrences
FROM gold.fact_sales
GROUP BY invoice_line_no
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;
```

## Recommended fix (production-grade)
Make the publish idempotent using an UPSERT (MERGE) on a business key.  
Preferred key: `invoice_line_no` (or `invoice_id + line_no` if modeled separately).

```sql
MERGE INTO gold.fact_sales AS t
USING gold.fact_sales_staging AS s
ON t.invoice_line_no = s.invoice_line_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Note: `gold.fact_sales_staging` is the freshly transformed dataset for the current run.

## Alternative (analytics-only workaround)
If you cannot modify the publish job, create a deduplicated view for analytics:

```sql
CREATE OR REPLACE VIEW gold.fact_sales_dedup AS
SELECT * EXCEPT (rn)
FROM (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY invoice_line_no
      ORDER BY year DESC, month DESC
    ) AS rn
  FROM gold.fact_sales f
)
WHERE rn = 1;
```

This keeps reporting stable, but the proper fix is the MERGE-based idempotent publish.
