import pandas as pd
import duckdb
import os
import logging
from datetime import datetime, timedelta
import numpy as np
import random

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBInitializer:
    def __init__(self, db_path='retail_data.db'):
        """Initialize the database."""
        self.db_path = db_path
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        
        # Ensure data directory exists
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
        # Connect to the database
        self.conn = duckdb.connect(os.path.join(self.data_dir, db_path))
        logger.info(f"Connected to database at {self.data_dir}/{db_path}")
        
    def initialize_database(self):
        """Initialize the database structure and load sample data."""
        try:
            # Create tables
            self._create_tables()
            
            # Generate sample data if not already generated
            self._generate_sample_data()
            
            # Load data into database
            self._load_data_to_db()
            
            logger.info("Database initialization completed successfully")
            return True
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False
            
    def _create_tables(self):
        """Create the database tables."""
        try:
            # Create stores table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS stores (
                    store_id INTEGER PRIMARY KEY,
                    store_name TEXT NOT NULL,
                    format TEXT,
                    region TEXT,
                    city TEXT,
                    open_date DATE,
                    size_sqm FLOAT,
                    is_active BOOLEAN
                )
            """)
            
            # Create products table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    category_id INTEGER,
                    subcategory_id INTEGER,
                    brand TEXT,
                    supplier_id INTEGER,
                    unit_price FLOAT,
                    unit_cost FLOAT,
                    unit_type TEXT,
                    is_private_label BOOLEAN
                )
            """)
            
            # Create categories table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    category_id INTEGER PRIMARY KEY,
                    category_name TEXT NOT NULL,
                    department TEXT
                )
            """)
            
            # Create subcategories table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS subcategories (
                    subcategory_id INTEGER PRIMARY KEY,
                    category_id INTEGER,
                    subcategory_name TEXT NOT NULL
                )
            """)
            
            # Create sales table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sales (
                    sale_id INTEGER PRIMARY KEY,
                    store_id INTEGER,
                    product_id INTEGER,
                    customer_id INTEGER,
                    sale_date DATE,
                    quantity FLOAT,
                    unit_price FLOAT,
                    discount FLOAT,
                    total_amount FLOAT,
                    payment_type TEXT,
                    promo_id INTEGER
                )
            """)
            
            # Create customers table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    registration_date DATE,
                    loyalty_level TEXT,
                    city TEXT,
                    birth_date DATE,
                    gender TEXT
                )
            """)
            
            # Create suppliers table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS suppliers (
                    supplier_id INTEGER PRIMARY KEY,
                    supplier_name TEXT NOT NULL,
                    contact_person TEXT,
                    email TEXT,
                    phone TEXT,
                    country TEXT,
                    rating FLOAT
                )
            """)
            
            # Create inventory table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS inventory (
                    inventory_id INTEGER PRIMARY KEY,
                    store_id INTEGER,
                    product_id INTEGER,
                    quantity FLOAT,
                    last_update TIMESTAMP,
                    min_stock_level FLOAT,
                    max_stock_level FLOAT
                )
            """)
            
            # Create promotions table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS promotions (
                    promo_id INTEGER PRIMARY KEY,
                    promo_name TEXT NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    promo_type TEXT,
                    discount_amount FLOAT,
                    min_purchase FLOAT
                )
            """)
            
            logger.info("All tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def _generate_sample_data(self):
        """Generate sample data for all tables if CSV files don't exist."""
        # Check if data files already exist
        if (os.path.exists(os.path.join(self.data_dir, 'stores.csv')) and
            os.path.exists(os.path.join(self.data_dir, 'products.csv')) and
            os.path.exists(os.path.join(self.data_dir, 'sales.csv'))):
            logger.info("Sample data files already exist, skipping generation")
            return
            
        logger.info("Generating sample data...")
        
        # Generate stores data (5 stores)
        stores = self._generate_stores_data()
        stores.to_csv(os.path.join(self.data_dir, 'stores.csv'), index=False)
        
        # Generate categories data
        categories = self._generate_categories_data()
        categories.to_csv(os.path.join(self.data_dir, 'categories.csv'), index=False)
        
        # Generate subcategories data
        subcategories = self._generate_subcategories_data(categories)
        subcategories.to_csv(os.path.join(self.data_dir, 'subcategories.csv'), index=False)
        
        # Generate suppliers data
        suppliers = self._generate_suppliers_data()
        suppliers.to_csv(os.path.join(self.data_dir, 'suppliers.csv'), index=False)
        
        # Generate products data (30 products)
        products = self._generate_products_data(categories, subcategories, suppliers)
        products.to_csv(os.path.join(self.data_dir, 'products.csv'), index=False)
        
        # Generate customers data
        customers = self._generate_customers_data()
        customers.to_csv(os.path.join(self.data_dir, 'customers.csv'), index=False)
        
        # Generate promotions data
        promotions = self._generate_promotions_data()
        promotions.to_csv(os.path.join(self.data_dir, 'promotions.csv'), index=False)
        
        # Generate sales data (100 records)
        sales = self._generate_sales_data(stores, products, customers, promotions)
        sales.to_csv(os.path.join(self.data_dir, 'sales.csv'), index=False)
        
        # Generate inventory data
        inventory = self._generate_inventory_data(stores, products)
        inventory.to_csv(os.path.join(self.data_dir, 'inventory.csv'), index=False)
        
        logger.info("Sample data generation completed")
        
    def _generate_stores_data(self):
        """Generate sample data for stores table."""
        data = {
            'store_id': range(1, 6),
            'store_name': [f"Магазин №{i}" for i in range(1, 6)],
            'format': ['гипермаркет', 'супермаркет', 'мини-маркет', 'супермаркет', 'мини-маркет'],
            'region': ['Москва', 'Санкт-Петербург', 'Краснодар', 'Москва', 'Санкт-Петербург'],
            'city': ['Москва', 'Санкт-Петербург', 'Краснодар', 'Москва', 'Санкт-Петербург'],
            'open_date': [
                (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d'),
                (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d'),
                (datetime.now() - timedelta(days=365*1)).strftime('%Y-%m-%d'),
                (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'),
                (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            ],
            'size_sqm': [1500, 800, 300, 750, 250],
            'is_active': [True, True, True, True, True]
        }
        return pd.DataFrame(data)
    
    def _generate_categories_data(self):
        """Generate sample data for categories table."""
        data = {
            'category_id': range(1, 6),
            'category_name': ['Молочные продукты', 'Хлебобулочные изделия', 'Мясо и птица', 'Напитки', 'Замороженные продукты'],
            'department': ['Продукты питания', 'Продукты питания', 'Продукты питания', 'Продукты питания', 'Продукты питания']
        }
        return pd.DataFrame(data)
    
    def _generate_subcategories_data(self, categories):
        """Generate sample data for subcategories table."""
        subcategories = []
        
        subcategory_map = {
            'Молочные продукты': ['Молоко', 'Сыр', 'Йогурт'],
            'Хлебобулочные изделия': ['Хлеб', 'Выпечка', 'Булочки'],
            'Мясо и птица': ['Говядина', 'Курица', 'Свинина'],
            'Напитки': ['Вода', 'Сок', 'Газировка'],
            'Замороженные продукты': ['Мороженое', 'Пельмени', 'Овощи замороженные']
        }
        
        subcategory_id = 1
        for idx, row in categories.iterrows():
            category_id = row['category_id']
            category_name = row['category_name']
            
            for subcategory_name in subcategory_map.get(category_name, []):
                subcategories.append({
                    'subcategory_id': subcategory_id,
                    'category_id': category_id,
                    'subcategory_name': subcategory_name
                })
                subcategory_id += 1
                
        return pd.DataFrame(subcategories)
    
    def _generate_suppliers_data(self):
        """Generate sample data for suppliers table."""
        data = {
            'supplier_id': range(1, 6),
            'supplier_name': ['МолокоПром', 'ХлебПром', 'МясоПром', 'НапиткиПром', 'ЗаморозкаПром'],
            'contact_person': ['Иванов И.И.', 'Петров П.П.', 'Сидоров С.С.', 'Козлова К.К.', 'Смирнов С.С.'],
            'email': ['info@molokoprom.ru', 'info@hlebprom.ru', 'info@myasoprom.ru', 'info@napitkiprom.ru', 'info@zamorozkaprom.ru'],
            'phone': ['+7(999)123-45-67', '+7(999)234-56-78', '+7(999)345-67-89', '+7(999)456-78-90', '+7(999)567-89-01'],
            'country': ['Россия', 'Россия', 'Россия', 'Россия', 'Беларусь'],
            'rating': [4.5, 4.7, 4.2, 4.6, 4.3]
        }
        return pd.DataFrame(data)
    
    def _generate_products_data(self, categories, subcategories, suppliers):
        """Generate sample data for products table."""
        products = []
        
        # Product data mapping: category_id -> [(product_name, brand, unit_type, is_private_label), ...]
        product_data = {
            1: [  # Молочные продукты
                ('Молоко 3,2%', 'Простоквашино', 'л', False),
                ('Сыр Российский', 'Cheese Gallery', 'кг', False),
                ('Йогурт клубничный', 'Danone', 'шт', False),
                ('Молоко 2,5%', 'ЧистаяЛиния', 'л', True),
                ('Творог 9%', 'Домик в деревне', 'кг', False),
                ('Сметана 15%', 'Простоквашино', 'кг', False)
            ],
            2: [  # Хлебобулочные изделия
                ('Хлеб белый', 'Хлебный дом', 'шт', False),
                ('Батон нарезной', 'Хлебный дом', 'шт', False),
                ('Булочка с маком', 'Каравай', 'шт', False),
                ('Лаваш', 'Восточный пекарь', 'шт', False),
                ('Хлеб бородинский', 'Хлебный дом', 'шт', False),
                ('Круассан', 'ТорговаяСеть', 'шт', True)
            ],
            3: [  # Мясо и птица
                ('Филе куриное', 'Петелинка', 'кг', False),
                ('Фарш говяжий', 'Мираторг', 'кг', False),
                ('Стейк свиной', 'Черкизово', 'кг', False),
                ('Колбаса вареная', 'Мясницкий ряд', 'кг', False),
                ('Сосиски молочные', 'ТорговаяСеть', 'кг', True),
                ('Окорочка куриные', 'Петелинка', 'кг', False)
            ],
            4: [  # Напитки
                ('Вода минеральная', 'BonAqua', 'л', False),
                ('Сок апельсиновый', 'Добрый', 'л', False),
                ('Газировка Кола', 'Coca-Cola', 'л', False),
                ('Вода питьевая', 'ТорговаяСеть', 'л', True),
                ('Сок яблочный', 'Я', 'л', False),
                ('Чай черный', 'Lipton', 'шт', False)
            ],
            5: [  # Замороженные продукты
                ('Мороженое пломбир', 'Чистая линия', 'шт', False),
                ('Пельмени Сибирские', 'Сибирская коллекция', 'кг', False),
                ('Овощная смесь', 'ТорговаяСеть', 'кг', True),
                ('Пицца замороженная', 'Dr. Oetker', 'шт', False),
                ('Вареники с вишней', 'Морозко', 'кг', False),
                ('Блинчики с мясом', 'Талосто', 'кг', False)
            ]
        }
        
        product_id = 1
        for category_id, product_list in product_data.items():
            # Get subcategories for this category
            category_subcategories = subcategories[subcategories['category_id'] == category_id]
            
            for product_name, brand, unit_type, is_private_label in product_list:
                # Randomly select a subcategory for this product
                if len(category_subcategories) > 0:
                    subcategory = category_subcategories.sample(1).iloc[0]
                    subcategory_id = subcategory['subcategory_id']
                else:
                    subcategory_id = None
                
                # Randomly select a supplier
                supplier_id = suppliers.sample(1).iloc[0]['supplier_id']
                
                # Generate price and cost
                unit_price = round(random.uniform(50, 500), 2)  # Between 50 and 500 rubles
                unit_cost = round(unit_price * random.uniform(0.5, 0.8), 2)  # 50-80% of the price
                
                products.append({
                    'product_id': product_id,
                    'product_name': product_name,
                    'category_id': category_id,
                    'subcategory_id': subcategory_id,
                    'brand': brand,
                    'supplier_id': supplier_id,
                    'unit_price': unit_price,
                    'unit_cost': unit_cost,
                    'unit_type': unit_type,
                    'is_private_label': is_private_label
                })
                
                product_id += 1
                
        return pd.DataFrame(products)
    
    def _generate_customers_data(self):
        """Generate sample data for customers table."""
        # First names and last names for random generation
        first_names = ['Александр', 'Сергей', 'Дмитрий', 'Андрей', 'Алексей', 
                      'Максим', 'Иван', 'Олег', 'Мария', 'Елена', 
                      'Анна', 'Ольга', 'Татьяна', 'Светлана', 'Наталья']
        
        last_names = ['Иванов', 'Смирнов', 'Кузнецов', 'Попов', 'Васильев',
                      'Петров', 'Соколов', 'Михайлов', 'Новиков', 'Федоров',
                      'Морозов', 'Волков', 'Алексеев', 'Лебедев', 'Семенов']
                      
        cities = ['Москва', 'Санкт-Петербург', 'Краснодар', 'Новосибирск', 'Екатеринбург']
        
        loyalty_levels = ['Бронза', 'Серебро', 'Золото', 'Платина']
        
        genders = ['М', 'Ж']
        
        customers = []
        
        for i in range(1, 21):  # Generate 20 customers
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            
            # Adjust last name for females
            gender = random.choice(genders)
            if gender == 'Ж' and last_name.endswith('ов'):
                last_name = last_name + 'а'
            elif gender == 'Ж' and last_name.endswith('ин'):
                last_name = last_name + 'а'
            
            registration_date = (datetime.now() - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d')
            birth_date = (datetime.now() - timedelta(days=365 * random.randint(18, 65))).strftime('%Y-%m-%d')
            
            customers.append({
                'customer_id': i,
                'first_name': first_name,
                'last_name': last_name,
                'email': f"{first_name.lower()}.{last_name.lower()}@example.com",
                'phone': f"+7(9{random.randint(10, 99)}{random.randint(100, 999)}{random.randint(10, 99)}{random.randint(10, 99)})",
                'registration_date': registration_date,
                'loyalty_level': random.choice(loyalty_levels),
                'city': random.choice(cities),
                'birth_date': birth_date,
                'gender': gender
            })
            
        return pd.DataFrame(customers)
    
    def _generate_promotions_data(self):
        """Generate sample data for promotions table."""
        now = datetime.now()
        
        promo_types = ['Скидка', '2+1', 'Купон', 'Подарок']
        
        data = {
            'promo_id': range(1, 6),
            'promo_name': ['Весенняя распродажа', 'Летняя акция', 'Черная пятница', 'Новогодняя скидка', 'День рождения'],
            'start_date': [
                (now - timedelta(days=60)).strftime('%Y-%m-%d'),
                (now - timedelta(days=30)).strftime('%Y-%m-%d'),
                (now - timedelta(days=15)).strftime('%Y-%m-%d'),
                (now - timedelta(days=5)).strftime('%Y-%m-%d'),
                (now).strftime('%Y-%m-%d')
            ],
            'end_date': [
                (now - timedelta(days=30)).strftime('%Y-%m-%d'),
                (now + timedelta(days=30)).strftime('%Y-%m-%d'),
                (now + timedelta(days=15)).strftime('%Y-%m-%d'),
                (now + timedelta(days=25)).strftime('%Y-%m-%d'),
                (now + timedelta(days=30)).strftime('%Y-%m-%d')
            ],
            'promo_type': [random.choice(promo_types) for _ in range(5)],
            'discount_amount': [random.uniform(0.05, 0.3) for _ in range(5)],  # 5% to 30% discount
            'min_purchase': [random.choice([0, 500, 1000, 1500, 2000]) for _ in range(5)]
        }
        return pd.DataFrame(data)
    
    def _generate_sales_data(self, stores, products, customers, promotions):
        """Generate sample data for sales table."""
        sales = []
        
        # Generate sales for the last 14 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=14)
        
        sale_id = 1
        
        # Generate ~100 sales
        for _ in range(100):
            # Random store
            store = stores.sample(1).iloc[0]
            
            # Random product
            product = products.sample(1).iloc[0]
            
            # Random customer (some sales might not have a customer)
            if random.random() > 0.2:  # 80% chance to have a customer
                customer = customers.sample(1).iloc[0]
                customer_id = customer['customer_id']
            else:
                customer_id = None
            
            # Random promo (some sales might not be part of a promotion)
            if random.random() > 0.7:  # 30% chance to have a promotion
                promo = promotions.sample(1).iloc[0]
                promo_id = promo['promo_id']
                discount = promo['discount_amount']
            else:
                promo_id = None
                discount = 0
            
            # Random date within range
            days_diff = (end_date - start_date).days
            sale_date = (start_date + timedelta(days=random.randint(0, days_diff))).strftime('%Y-%m-%d')
            
            # Random quantity
            quantity = random.randint(1, 5)
            
            # Calculate price (may be discounted)
            unit_price = product['unit_price'] * (1 - discount)
            
            # Calculate total
            total_amount = unit_price * quantity
            
            # Payment type
            payment_type = random.choice(['Наличные', 'Карта', 'Онлайн'])
            
            sales.append({
                'sale_id': sale_id,
                'store_id': store['store_id'],
                'product_id': product['product_id'],
                'customer_id': customer_id,
                'sale_date': sale_date,
                'quantity': quantity,
                'unit_price': unit_price,
                'discount': discount,
                'total_amount': total_amount,
                'payment_type': payment_type,
                'promo_id': promo_id
            })
            
            sale_id += 1
            
        return pd.DataFrame(sales)
    
    def _generate_inventory_data(self, stores, products):
        """Generate sample data for inventory table."""
        inventory = []
        
        inventory_id = 1
        
        # For each store and product combination
        for _, store in stores.iterrows():
            for _, product in products.iterrows():
                # Random current quantity
                quantity = random.randint(0, 100)
                
                # Min and max stock levels
                min_stock_level = random.randint(5, 20)
                max_stock_level = random.randint(50, 150)
                
                # Last update timestamp
                last_update = (datetime.now() - timedelta(hours=random.randint(0, 48))).strftime('%Y-%m-%d %H:%M:%S')
                
                inventory.append({
                    'inventory_id': inventory_id,
                    'store_id': store['store_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'last_update': last_update,
                    'min_stock_level': min_stock_level,
                    'max_stock_level': max_stock_level
                })
                
                inventory_id += 1
                
        return pd.DataFrame(inventory)
    
    def _load_data_to_db(self):
        """Load the generated data into the database."""
        try:
            # Load stores data
            stores_df = pd.read_csv(os.path.join(self.data_dir, 'stores.csv'))
            self.conn.execute("DELETE FROM stores")
            self.conn.execute("INSERT INTO stores SELECT * FROM stores_df")
            
            # Load categories data
            categories_df = pd.read_csv(os.path.join(self.data_dir, 'categories.csv'))
            self.conn.execute("DELETE FROM categories")
            self.conn.execute("INSERT INTO categories SELECT * FROM categories_df")
            
            # Load subcategories data
            subcategories_df = pd.read_csv(os.path.join(self.data_dir, 'subcategories.csv'))
            self.conn.execute("DELETE FROM subcategories")
            self.conn.execute("INSERT INTO subcategories SELECT * FROM subcategories_df")
            
            # Load suppliers data
            suppliers_df = pd.read_csv(os.path.join(self.data_dir, 'suppliers.csv'))
            self.conn.execute("DELETE FROM suppliers")
            self.conn.execute("INSERT INTO suppliers SELECT * FROM suppliers_df")
            
            # Load products data
            products_df = pd.read_csv(os.path.join(self.data_dir, 'products.csv'))
            self.conn.execute("DELETE FROM products")
            self.conn.execute("INSERT INTO products SELECT * FROM products_df")
            
            # Load customers data
            customers_df = pd.read_csv(os.path.join(self.data_dir, 'customers.csv'))
            self.conn.execute("DELETE FROM customers")
            self.conn.execute("INSERT INTO customers SELECT * FROM customers_df")
            
            # Load promotions data
            promotions_df = pd.read_csv(os.path.join(self.data_dir, 'promotions.csv'))
            self.conn.execute("DELETE FROM promotions")
            self.conn.execute("INSERT INTO promotions SELECT * FROM promotions_df")
            
            # Load sales data
            sales_df = pd.read_csv(os.path.join(self.data_dir, 'sales.csv'))
            self.conn.execute("DELETE FROM sales")
            self.conn.execute("INSERT INTO sales SELECT * FROM sales_df")
            
            # Load inventory data
            inventory_df = pd.read_csv(os.path.join(self.data_dir, 'inventory.csv'))
            self.conn.execute("DELETE FROM inventory")
            self.conn.execute("INSERT INTO inventory SELECT * FROM inventory_df")
            
            logger.info("All data loaded into database successfully")
            
        except Exception as e:
            logger.error(f"Error loading data into database: {e}")
            raise

if __name__ == "__main__":
    # If run directly, initialize the database
    initializer = DBInitializer()
    initializer.initialize_database() 