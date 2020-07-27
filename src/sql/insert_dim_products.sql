INSERT INTO bi.dim_products
        Select product_id, 
        product_name, 
        model_year, 
        list_price
        FROM production.products