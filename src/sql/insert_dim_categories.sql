INSERT INTO bi.dim_categories
SELECT category_id, category_name
FROM production.categories