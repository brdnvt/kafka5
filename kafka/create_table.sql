CREATE TABLE IF NOT EXISTS coffee_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT,
    size TEXT,
    milk INTEGER,
    whip INTEGER,
    serv_size_m_l INTEGER,
    calories INTEGER,
    total_fat_g NUMERIC,
    saturated_fat_g NUMERIC,
    trans_fat_g NUMERIC,
    cholesterol_mg INTEGER,
    sodium_mg INTEGER,
    total_carbs_g NUMERIC,
    fiber_g NUMERIC,
    sugar_g NUMERIC,
    caffeine_mg INTEGER
);
