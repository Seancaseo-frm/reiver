-- SQL Server seed data: Inventory table
-- Join key: product_id (references MongoDB products.product_id)

-- Create testdb database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
    CREATE DATABASE testdb;
END
GO

USE testdb;
GO

-- Create inventory table
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventory' AND xtype='U')
BEGIN
    CREATE TABLE inventory (
        id INT IDENTITY(1,1) PRIMARY KEY,
        product_id VARCHAR(50) NOT NULL,
        warehouse_code VARCHAR(20) NOT NULL,
        warehouse_name VARCHAR(100) NOT NULL,
        quantity_on_hand INT NOT NULL DEFAULT 0,
        quantity_reserved INT NOT NULL DEFAULT 0,
        quantity_available AS (quantity_on_hand - quantity_reserved),
        reorder_point INT NOT NULL DEFAULT 10,
        reorder_quantity INT NOT NULL DEFAULT 50,
        unit_cost DECIMAL(10, 2) NOT NULL,
        total_value AS (quantity_on_hand * unit_cost),
        last_received_date DATETIME NULL,
        last_sold_date DATETIME NULL,
        location_aisle VARCHAR(10),
        location_shelf VARCHAR(10),
        location_bin VARCHAR(10),
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        
        CONSTRAINT UQ_inventory_product_warehouse UNIQUE (product_id, warehouse_code)
    );

    CREATE INDEX idx_inventory_product_id ON inventory(product_id);
    CREATE INDEX idx_inventory_warehouse_code ON inventory(warehouse_code);
    CREATE INDEX idx_inventory_quantity_available ON inventory(quantity_on_hand, quantity_reserved);
END
GO

-- Insert 200 inventory records (4 warehouses x 50 products)
DECLARE @i INT = 1;
DECLARE @product_num INT;
DECLARE @warehouse_num INT;
DECLARE @product_id VARCHAR(50);
DECLARE @warehouse_code VARCHAR(20);
DECLARE @warehouse_name VARCHAR(100);
DECLARE @quantity INT;
DECLARE @reserved INT;
DECLARE @unit_cost DECIMAL(10,2);

WHILE @i <= 200
BEGIN
    SET @product_num = ((@i - 1) % 50) + 1;
    SET @warehouse_num = ((@i - 1) / 50) + 1;
    SET @product_id = 'P' + RIGHT('000' + CAST(@product_num AS VARCHAR(3)), 3);
    SET @warehouse_code = 'WH-' + RIGHT('00' + CAST(@warehouse_num AS VARCHAR(2)), 2);
    SET @warehouse_name = CASE @warehouse_num
        WHEN 1 THEN 'East Coast Distribution Center'
        WHEN 2 THEN 'West Coast Distribution Center'
        WHEN 3 THEN 'Central Hub Warehouse'
        WHEN 4 THEN 'International Fulfillment Center'
    END;
    SET @quantity = 100 + (@i * 3);
    SET @reserved = @i % 20;
    SET @unit_cost = 5.00 + (@product_num * 1.25);

    INSERT INTO inventory (
        product_id, 
        warehouse_code, 
        warehouse_name, 
        quantity_on_hand, 
        quantity_reserved,
        reorder_point,
        reorder_quantity,
        unit_cost,
        last_received_date,
        last_sold_date,
        location_aisle,
        location_shelf,
        location_bin,
        is_active
    )
    VALUES (
        @product_id,
        @warehouse_code,
        @warehouse_name,
        @quantity,
        @reserved,
        10 + (@product_num % 10),
        50 + (@product_num % 30),
        @unit_cost,
        DATEADD(DAY, -(@i % 30), GETUTCDATE()),
        CASE WHEN @i % 5 = 0 THEN NULL ELSE DATEADD(DAY, -(@i % 7), GETUTCDATE()) END,
        CHAR(65 + (@i % 10)),  -- A-J
        CAST(1 + (@i % 5) AS VARCHAR(10)),
        CAST(1 + (@i % 20) AS VARCHAR(10)),
        CASE WHEN @i % 25 = 0 THEN 0 ELSE 1 END
    );

    SET @i = @i + 1;
END
GO

-- Create a view for easy querying
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_inventory_summary')
    DROP VIEW v_inventory_summary;
GO

CREATE VIEW v_inventory_summary AS
SELECT 
    product_id,
    SUM(quantity_on_hand) AS total_on_hand,
    SUM(quantity_reserved) AS total_reserved,
    SUM(quantity_on_hand - quantity_reserved) AS total_available,
    COUNT(DISTINCT warehouse_code) AS warehouse_count,
    AVG(unit_cost) AS avg_unit_cost,
    SUM(quantity_on_hand * unit_cost) AS total_inventory_value
FROM inventory
WHERE is_active = 1
GROUP BY product_id;
GO

-- Verify data
SELECT 'SQL Server inventory table seeded with' AS message, COUNT(*) AS row_count FROM inventory;
GO
