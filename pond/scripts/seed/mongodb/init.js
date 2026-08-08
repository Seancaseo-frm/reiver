// MongoDB seed data: Products collection
// Join key: product_id (referenced by MySQL orders.product_id)
// Join key: product_id (referenced by SQL Server inventory.product_id)

// Switch to testdb
db = db.getSiblingDB('testdb');

// Create products collection with schema validation
db.createCollection('products', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            required: ['product_id', 'name', 'category', 'price'],
            properties: {
                product_id: {
                    bsonType: 'string',
                    description: 'Unique product identifier'
                },
                name: {
                    bsonType: 'string',
                    description: 'Product name'
                },
                category: {
                    bsonType: 'string',
                    description: 'Product category'
                },
                price: {
                    bsonType: 'decimal',
                    description: 'Product price'
                }
            }
        }
    }
});

// Create indexes
db.products.createIndex({ product_id: 1 }, { unique: true });
db.products.createIndex({ category: 1 });
db.products.createIndex({ price: 1 });
db.products.createIndex({ 'specifications.brand': 1 });

// Categories and brands for deterministic data generation
const categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books'];
const brands = ['TechCo', 'StyleBrand', 'HomePlus', 'SportPro', 'ReadMore'];
const colors = ['Red', 'Blue', 'Green', 'Black', 'White', 'Silver', 'Gold'];

// Generate 50 products
const products = [];
for (let i = 1; i <= 50; i++) {
    const categoryIndex = (i - 1) % 5;
    const product = {
        product_id: 'P' + String(i).padStart(3, '0'),
        name: `Product ${i} - ${categories[categoryIndex]}`,
        description: `This is a high-quality ${categories[categoryIndex].toLowerCase()} product. Item number ${i}.`,
        category: categories[categoryIndex],
        subcategory: `${categories[categoryIndex]} Sub-${((i - 1) % 3) + 1}`,
        price: NumberDecimal(String((10 + (i * 3.5)).toFixed(2))),
        cost: NumberDecimal(String((5 + (i * 1.5)).toFixed(2))),
        sku: `SKU-${String(i).padStart(6, '0')}`,
        barcode: `1234567${String(i).padStart(5, '0')}`,
        weight_kg: parseFloat((0.5 + (i * 0.1)).toFixed(2)),
        dimensions: {
            length_cm: 10 + (i % 20),
            width_cm: 5 + (i % 15),
            height_cm: 2 + (i % 10)
        },
        specifications: {
            brand: brands[categoryIndex],
            color: colors[(i - 1) % 7],
            material: i % 2 === 0 ? 'Premium' : 'Standard',
            warranty_months: 12 + ((i % 4) * 6)
        },
        tags: [
            categories[categoryIndex].toLowerCase(),
            brands[categoryIndex].toLowerCase(),
            i % 3 === 0 ? 'sale' : 'regular',
            i % 5 === 0 ? 'featured' : 'standard'
        ],
        ratings: {
            average: parseFloat((3.5 + (i % 15) * 0.1).toFixed(1)),
            count: 10 + (i * 7)
        },
        inventory: {
            in_stock: i % 10 !== 0,
            quantity: i % 10 === 0 ? 0 : 50 + (i * 3),
            reorder_level: 10,
            warehouse_location: `WH-${String((i % 5) + 1).padStart(2, '0')}`
        },
        is_active: i % 15 !== 0,
        created_at: new Date(Date.now() - (50 - i) * 24 * 60 * 60 * 1000),
        updated_at: new Date()
    };
    products.push(product);
}

// Insert products
db.products.insertMany(products);

// Verify data
print('MongoDB products collection seeded with ' + db.products.countDocuments() + ' documents');

// Create a test user for the application
db.createUser({
    user: 'testuser',
    pwd: 'testpass',
    roles: [
        { role: 'readWrite', db: 'testdb' }
    ]
});

print('MongoDB initialization complete');
