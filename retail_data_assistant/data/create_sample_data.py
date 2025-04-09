import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import random

# Убедимся, что директория data существует
data_dir = os.path.dirname(os.path.abspath(__file__))
os.makedirs(data_dir, exist_ok=True)

# 1. Таблица магазинов (stores)
def generate_stores():
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

# 2. Таблица категорий (categories)
def generate_categories():
    data = {
        'category_id': range(1, 6),
        'category_name': ['Молочные продукты', 'Хлебобулочные изделия', 'Мясо и птица', 'Напитки', 'Замороженные продукты'],
        'department': ['Продукты питания', 'Продукты питания', 'Продукты питания', 'Продукты питания', 'Продукты питания']
    }
    return pd.DataFrame(data)

# 3. Таблица подкатегорий (subcategories)
def generate_subcategories(categories):
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

# 4. Таблица поставщиков (suppliers)
def generate_suppliers():
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

# 5. Таблица товаров (products)
def generate_products(categories, subcategories, suppliers):
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

# 6. Таблица клиентов (customers)
def generate_customers():
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

# 7. Таблица промо-акций (promotions)
def generate_promotions():
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

# 8. Таблица продаж (sales)
def generate_sales(stores, products, customers, promotions):
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

# 9. Таблица запасов (inventory)
def generate_inventory(stores, products):
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

# Генерация данных и сохранение в CSV
def generate_all_data():
    print("Generating sample data...")
    
    # Generate data for all tables
    stores_df = generate_stores()
    stores_df.to_csv(os.path.join(data_dir, 'stores.csv'), index=False)
    print("Generated stores.csv")
    
    categories_df = generate_categories()
    categories_df.to_csv(os.path.join(data_dir, 'categories.csv'), index=False)
    print("Generated categories.csv")
    
    subcategories_df = generate_subcategories(categories_df)
    subcategories_df.to_csv(os.path.join(data_dir, 'subcategories.csv'), index=False)
    print("Generated subcategories.csv")
    
    suppliers_df = generate_suppliers()
    suppliers_df.to_csv(os.path.join(data_dir, 'suppliers.csv'), index=False)
    print("Generated suppliers.csv")
    
    products_df = generate_products(categories_df, subcategories_df, suppliers_df)
    products_df.to_csv(os.path.join(data_dir, 'products.csv'), index=False)
    print("Generated products.csv")
    
    customers_df = generate_customers()
    customers_df.to_csv(os.path.join(data_dir, 'customers.csv'), index=False)
    print("Generated customers.csv")
    
    promotions_df = generate_promotions()
    promotions_df.to_csv(os.path.join(data_dir, 'promotions.csv'), index=False)
    print("Generated promotions.csv")
    
    sales_df = generate_sales(stores_df, products_df, customers_df, promotions_df)
    sales_df.to_csv(os.path.join(data_dir, 'sales.csv'), index=False)
    print("Generated sales.csv")
    
    inventory_df = generate_inventory(stores_df, products_df)
    inventory_df.to_csv(os.path.join(data_dir, 'inventory.csv'), index=False)
    print("Generated inventory.csv")
    
    print("Sample data generation completed successfully!")

if __name__ == "__main__":
    generate_all_data() 