-- MySQL seed data: Orders table
-- Join key: customer_id (references PostgreSQL customers.id)
-- Join key: product_id (references MongoDB products.product_id)

USE testdb;

CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') NOT NULL DEFAULT 'pending',
    order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    shipped_date DATETIME NULL,
    notes TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_customer_id (customer_id),
    INDEX idx_product_id (product_id),
    INDEX idx_order_date (order_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert 1000 orders with deterministic data for reproducible tests
-- Customer IDs 1-100 (matching PostgreSQL customers)
-- Product IDs P001-P050 (matching MongoDB products)

DELIMITER //

CREATE PROCEDURE seed_orders()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE customer_id INT;
    DECLARE product_id VARCHAR(50);
    DECLARE quantity INT;
    DECLARE unit_price DECIMAL(10,2);
    DECLARE status_val VARCHAR(20);
    DECLARE order_date DATETIME;
    DECLARE statuses VARCHAR(100) DEFAULT 'pending,processing,shipped,delivered,cancelled';
    
    WHILE i <= 1000 DO
        SET customer_id = ((i - 1) MOD 100) + 1;
        SET product_id = CONCAT('P', LPAD(((i - 1) MOD 50) + 1, 3, '0'));
        SET quantity = (i MOD 5) + 1;
        SET unit_price = 10.00 + ((i MOD 100) * 1.50);
        SET status_val = ELT(((i - 1) MOD 5) + 1, 'pending', 'processing', 'shipped', 'delivered', 'cancelled');
        SET order_date = DATE_SUB(NOW(), INTERVAL (1000 - i) HOUR);
        
        INSERT INTO orders (order_number, customer_id, product_id, quantity, unit_price, total_amount, status, order_date, shipped_date, notes)
        VALUES (
            CONCAT('ORD-', LPAD(i, 6, '0')),
            customer_id,
            product_id,
            quantity,
            unit_price,
            quantity * unit_price,
            status_val,
            order_date,
            CASE WHEN status_val IN ('shipped', 'delivered') THEN DATE_ADD(order_date, INTERVAL 2 DAY) ELSE NULL END,
            CASE WHEN i MOD 10 = 0 THEN CONCAT('Note for order ', i) ELSE NULL END
        );
        
        SET i = i + 1;
    END WHILE;
END //

DELIMITER ;

CALL seed_orders();
DROP PROCEDURE seed_orders;

-- Verify data
SELECT 'MySQL orders table seeded with' AS message, COUNT(*) AS row_count FROM orders;
