-- PostgreSQL seed data: Customers table
-- Join key: id (referenced by MySQL orders.customer_id)

CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    customer_code VARCHAR(20) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    company VARCHAR(200),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'United States',
    tier VARCHAR(20) DEFAULT 'standard' CHECK (tier IN ('standard', 'premium', 'enterprise')),
    is_active BOOLEAN DEFAULT true,
    total_orders INT DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_tier ON customers(tier);
CREATE INDEX IF NOT EXISTS idx_customers_is_active ON customers(is_active);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at);

-- Insert 100 customers with deterministic data
INSERT INTO customers (customer_code, first_name, last_name, email, phone, company, address_line1, city, state, postal_code, country, tier, is_active, total_orders, total_spent)
SELECT
    'CUST-' || LPAD(n::text, 4, '0'),
    (ARRAY['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'William', 'Jennifer'])[((n - 1) % 10) + 1],
    (ARRAY['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'])[((n - 1) % 10) + 1],
    'customer' || n || '@example.com',
    '+1-555-' || LPAD((1000 + n)::text, 4, '0'),
    CASE WHEN n % 3 = 0 THEN 'Company ' || (n / 3) ELSE NULL END,
    n || ' Main Street',
    (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin'])[((n - 1) % 10) + 1],
    (ARRAY['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'TX'])[((n - 1) % 10) + 1],
    LPAD((10000 + n * 100)::text, 5, '0'),
    'United States',
    (ARRAY['standard', 'premium', 'enterprise'])[((n - 1) % 3) + 1],
    n % 20 != 0,  -- 5% inactive
    (n * 7) % 50,  -- Random-ish order count
    ((n * 123.45) % 10000)::decimal(12,2)  -- Random-ish total spent
FROM generate_series(1, 100) AS n
ON CONFLICT (customer_code) DO NOTHING;

-- Verify data
DO $$
DECLARE
    cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO cnt FROM customers;
    RAISE NOTICE 'PostgreSQL customers table seeded with % rows', cnt;
END $$;
