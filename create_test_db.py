#!/usr/bin/env python3
"""Create a SQLite test database with sample e-commerce data for Schema Scribe testing."""

import sqlite3
import os
import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker for generating realistic data
fake = Faker()

def create_test_database(db_path: str = "test_ecommerce.db"):
    """Create a comprehensive test database with realistic e-commerce data."""
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"🗑️  Removed existing database: {db_path}")
    
    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"🏗️  Creating test database: {db_path}")
    
    # Create tables with realistic schema
    create_tables(cursor)
    print("✅ Created database tables")
    
    # Insert sample data
    insert_sample_data(cursor, conn)
    print("✅ Inserted sample data")
    
    # Create indexes for performance
    create_indexes(cursor)
    print("✅ Created database indexes")
    
    conn.commit()
    conn.close()
    
    print(f"🎉 Test database created successfully: {db_path}")
    print(f"📊 Database file size: {os.path.getsize(db_path):,} bytes")
    return db_path

def create_tables(cursor):
    """Create all database tables."""
    
    # Customers table
    cursor.execute('''
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            date_of_birth DATE,
            registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            customer_segment TEXT CHECK(customer_segment IN ('bronze', 'silver', 'gold', 'platinum')),
            total_lifetime_value DECIMAL(10,2) DEFAULT 0.00,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Products table
    cursor.execute('''
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            product_category TEXT NOT NULL,
            brand TEXT,
            price DECIMAL(10,2) NOT NULL,
            cost DECIMAL(10,2),
            weight_kg DECIMAL(8,3),
            dimensions_cm TEXT,
            stock_quantity INTEGER DEFAULT 0,
            reorder_level INTEGER DEFAULT 10,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_discontinued BOOLEAN DEFAULT 0
        )
    ''')
    
    # Orders table
    cursor.execute('''
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ship_date TIMESTAMP,
            delivery_date TIMESTAMP,
            order_status TEXT CHECK(order_status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
            shipping_address TEXT,
            billing_address TEXT,
            payment_method TEXT,
            subtotal DECIMAL(10,2),
            tax_amount DECIMAL(10,2),
            shipping_cost DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            discount_amount DECIMAL(10,2) DEFAULT 0.00,
            order_source TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    ''')
    
    # Order items table
    cursor.execute('''
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            line_total DECIMAL(10,2) NOT NULL,
            discount_applied DECIMAL(10,2) DEFAULT 0.00,
            FOREIGN KEY (order_id) REFERENCES orders (order_id),
            FOREIGN KEY (product_id) REFERENCES products (product_id)
        )
    ''')
    
    # Reviews table
    cursor.execute('''
        CREATE TABLE product_reviews (
            review_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            order_id INTEGER,
            rating INTEGER CHECK(rating BETWEEN 1 AND 5),
            review_title TEXT,
            review_text TEXT,
            review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_verified_purchase BOOLEAN DEFAULT 0,
            helpful_votes INTEGER DEFAULT 0,
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
            FOREIGN KEY (order_id) REFERENCES orders (order_id)
        )
    ''')
    
    # Inventory tracking table
    cursor.execute('''
        CREATE TABLE inventory_movements (
            movement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            movement_type TEXT CHECK(movement_type IN ('inbound', 'outbound', 'adjustment')),
            quantity_change INTEGER NOT NULL,
            movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reference_order_id INTEGER,
            notes TEXT,
            unit_cost DECIMAL(10,2),
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (reference_order_id) REFERENCES orders (order_id)
        )
    ''')
    
    # Marketing campaigns table
    cursor.execute('''
        CREATE TABLE marketing_campaigns (
            campaign_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT NOT NULL,
            campaign_type TEXT,
            start_date DATE,
            end_date DATE,
            budget DECIMAL(12,2),
            target_audience TEXT,
            channel TEXT,
            conversion_rate DECIMAL(5,4),
            total_clicks INTEGER DEFAULT 0,
            total_impressions INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT 1
        )
    ''')

def insert_sample_data(cursor, conn):
    """Insert realistic sample data into all tables."""
    
    # Insert customers
    print("  📝 Inserting customers...")
    customers_data = []
    for i in range(500):
        customer = (
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number(),
            fake.date_of_birth(minimum_age=18, maximum_age=80),
            fake.date_time_between(start_date='-2y', end_date='now'),
            random.choice(['bronze', 'silver', 'gold', 'platinum']),
            round(random.uniform(50, 5000), 2),
            random.choice([True, False]) if random.random() > 0.1 else True  # 90% active
        )
        customers_data.append(customer)
    
    cursor.executemany('''
        INSERT INTO customers (first_name, last_name, email, phone, date_of_birth, 
                             registration_date, customer_segment, total_lifetime_value, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', customers_data)
    
    # Insert products
    print("  📦 Inserting products...")
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Beauty', 'Automotive']
    brands = ['TechCorp', 'StyleBrand', 'HomeMax', 'SportsPro', 'ReadWell', 'PlayTime', 'BeautyPlus', 'AutoExpert']
    
    products_data = []
    for i in range(200):
        category = random.choice(categories)
        product = (
            fake.catch_phrase() + f" {category}",
            category,
            random.choice(brands),
            round(random.uniform(10, 500), 2),
            round(random.uniform(5, 250), 2),
            round(random.uniform(0.1, 10), 3),
            f"{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,30)}",
            random.randint(0, 100),
            random.randint(5, 20),
            fake.date_time_between(start_date='-1y', end_date='now'),
            fake.date_time_between(start_date='-30d', end_date='now'),
            random.choice([True, False]) if random.random() > 0.05 else False  # 5% discontinued
        )
        products_data.append(product)
    
    cursor.executemany('''
        INSERT INTO products (product_name, product_category, brand, price, cost, weight_kg,
                            dimensions_cm, stock_quantity, reorder_level, created_date, 
                            last_updated, is_discontinued)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', products_data)
    
    # Get customer and product IDs for foreign key references
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id, price FROM products")
    products = cursor.fetchall()
    
    # Insert orders
    print("  🛒 Inserting orders...")
    orders_data = []
    order_items_data = []
    
    for i in range(1000):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-6m', end_date='now')
        
        # Calculate shipping and delivery dates
        ship_date = None
        delivery_date = None
        status = random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled'])
        
        if status in ['shipped', 'delivered']:
            ship_date = order_date + timedelta(days=random.randint(1, 3))
        if status == 'delivered':
            delivery_date = ship_date + timedelta(days=random.randint(1, 7))
        
        # Calculate order totals
        num_items = random.randint(1, 5)
        subtotal = 0
        
        order = (
            customer_id,
            order_date,
            ship_date,
            delivery_date,
            status,
            fake.address(),
            fake.address(),
            random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
            random.choice(['website', 'mobile_app', 'phone', 'store'])
        )
        orders_data.append(order)
        order_id = 1000 + i + 1  # Assuming auto-increment starts at 1
        
        # Add order items
        selected_products = random.sample(products, min(num_items, len(products)))
        for product_id, price in selected_products:
            quantity = random.randint(1, 3)
            line_total = quantity * price
            subtotal += line_total
            
            order_item = (
                order_id,
                product_id,
                quantity,
                price,
                line_total,
                round(random.uniform(0, line_total * 0.1), 2)  # Random discount
            )
            order_items_data.append(order_item)
        
        # Update order with calculated totals
        tax_rate = 0.08
        tax_amount = round(subtotal * tax_rate, 2)
        shipping_cost = round(random.uniform(5, 25), 2) if subtotal < 100 else 0
        discount = round(random.uniform(0, subtotal * 0.15), 2)
        total = subtotal + tax_amount + shipping_cost - discount
        
        # Update the order tuple with calculated values
        orders_data[-1] = order + (subtotal, tax_amount, shipping_cost, total, discount)
    
    cursor.executemany('''
        INSERT INTO orders (customer_id, order_date, ship_date, delivery_date, order_status,
                          shipping_address, billing_address, payment_method, order_source,
                          subtotal, tax_amount, shipping_cost, total_amount, discount_amount)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', orders_data)
    
    # Insert order items
    print("  📋 Inserting order items...")
    cursor.executemany('''
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total, discount_applied)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', order_items_data)
    
    # Insert product reviews
    print("  ⭐ Inserting product reviews...")
    reviews_data = []
    for i in range(300):
        review = (
            random.choice([p[0] for p in products]),
            random.choice(customer_ids),
            random.randint(1000, 2000) if random.random() > 0.3 else None,  # 70% have order reference
            random.randint(1, 5),
            fake.sentence(nb_words=6),
            fake.paragraph(nb_sentences=3),
            fake.date_time_between(start_date='-4m', end_date='now'),
            random.choice([True, False]),
            random.randint(0, 50)
        )
        reviews_data.append(review)
    
    cursor.executemany('''
        INSERT INTO product_reviews (product_id, customer_id, order_id, rating, review_title,
                                   review_text, review_date, is_verified_purchase, helpful_votes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', reviews_data)
    
    # Insert inventory movements
    print("  📊 Inserting inventory movements...")
    movements_data = []
    for i in range(400):
        movement = (
            random.choice([p[0] for p in products]),
            random.choice(['inbound', 'outbound', 'adjustment']),
            random.randint(-50, 100),
            fake.date_time_between(start_date='-3m', end_date='now'),
            random.randint(1000, 2000) if random.random() > 0.5 else None,
            fake.sentence() if random.random() > 0.7 else None,
            round(random.uniform(5, 100), 2)
        )
        movements_data.append(movement)
    
    cursor.executemany('''
        INSERT INTO inventory_movements (product_id, movement_type, quantity_change, movement_date,
                                       reference_order_id, notes, unit_cost)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', movements_data)
    
    # Insert marketing campaigns
    print("  📢 Inserting marketing campaigns...")
    campaigns_data = []
    for i in range(20):
        start_date = fake.date_between(start_date='-6m', end_date='now')
        end_date = start_date + timedelta(days=random.randint(7, 90))
        
        campaign = (
            fake.catch_phrase() + " Campaign",
            random.choice(['email', 'social_media', 'ppc', 'display', 'influencer']),
            start_date,
            end_date,
            round(random.uniform(1000, 50000), 2),
            random.choice(['18-25', '26-35', '36-50', '50+']),
            random.choice(['facebook', 'google', 'instagram', 'email', 'youtube']),
            round(random.uniform(0.01, 0.15), 4),
            random.randint(100, 10000),
            random.randint(1000, 100000),
            end_date > datetime.now().date()
        )
        campaigns_data.append(campaign)
    
    cursor.executemany('''
        INSERT INTO marketing_campaigns (campaign_name, campaign_type, start_date, end_date,
                                       budget, target_audience, channel, conversion_rate,
                                       total_clicks, total_impressions, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', campaigns_data)

def create_indexes(cursor):
    """Create database indexes for better performance."""
    indexes = [
        "CREATE INDEX idx_customers_email ON customers(email)",
        "CREATE INDEX idx_customers_segment ON customers(customer_segment)",
        "CREATE INDEX idx_orders_customer_id ON orders(customer_id)",
        "CREATE INDEX idx_orders_date ON orders(order_date)",
        "CREATE INDEX idx_orders_status ON orders(order_status)",
        "CREATE INDEX idx_order_items_order_id ON order_items(order_id)",
        "CREATE INDEX idx_order_items_product_id ON order_items(product_id)",
        "CREATE INDEX idx_products_category ON products(product_category)",
        "CREATE INDEX idx_reviews_product_id ON product_reviews(product_id)",
        "CREATE INDEX idx_reviews_rating ON product_reviews(rating)",
        "CREATE INDEX idx_inventory_product_id ON inventory_movements(product_id)"
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)

def print_database_stats(db_path: str):
    """Print statistics about the created database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"\n📈 Database Statistics for {db_path}:")
    print("=" * 50)
    
    tables = ['customers', 'products', 'orders', 'order_items', 'product_reviews', 
              'inventory_movements', 'marketing_campaigns']
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table.ljust(20)}: {count:,} rows")
    
    # Show some sample data
    print(f"\n🔍 Sample Data Preview:")
    print("-" * 30)
    
    cursor.execute("""
        SELECT c.first_name || ' ' || c.last_name as customer_name, 
               COUNT(o.order_id) as total_orders,
               ROUND(SUM(o.total_amount), 2) as total_spent
        FROM customers c 
        LEFT JOIN orders o ON c.customer_id = o.customer_id 
        GROUP BY c.customer_id 
        ORDER BY total_spent DESC 
        LIMIT 5
    """)
    
    print("Top 5 Customers by Total Spending:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} orders, ${row[2]}")
    
    cursor.execute("""
        SELECT p.product_name, p.product_category, 
               COUNT(oi.order_item_id) as times_ordered,
               ROUND(AVG(pr.rating), 1) as avg_rating
        FROM products p
        LEFT JOIN order_items oi ON p.product_id = oi.product_id
        LEFT JOIN product_reviews pr ON p.product_id = pr.product_id
        GROUP BY p.product_id
        ORDER BY times_ordered DESC
        LIMIT 5
    """)
    
    print("\nTop 5 Most Ordered Products:")
    for row in cursor.fetchall():
        rating = row[3] if row[3] else "No ratings"
        print(f"  {row[0]} ({row[1]}): {row[2]} orders, {rating} ⭐")
    
    conn.close()

def main():
    """Main function to create and display test database."""
    print("🚀 Schema Scribe SQLite Test Database Creator")
    print("=" * 50)
    
    try:
        # Install faker if not available
        try:
            from faker import Faker
        except ImportError:
            print("📦 Installing faker for realistic test data generation...")
            import subprocess
            subprocess.check_call(["pip", "install", "faker"])
            from faker import Faker
        
        # Create the database
        db_path = create_test_database()
        
        # Print statistics
        print_database_stats(db_path)
        
        print(f"\n🎯 Next Steps:")
        print(f"1. Start Schema Scribe: python -m schema_scribe.main")
        print(f"2. Open http://localhost:8000")
        print(f"3. Add data source with connection string: sqlite:///{os.path.abspath(db_path)}")
        print(f"4. Run schema discovery and generate AI descriptions!")
        print(f"5. To clean up: rm {db_path}")
        
    except Exception as e:
        print(f"❌ Error creating test database: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())