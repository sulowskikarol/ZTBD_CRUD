import csv
import os
import time
import random
import mysql.connector
from datetime import datetime, timezone
from pathlib import Path
import shutil

DATA_DIR = "data"
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# 🔐 Dane połączenia z MySQL
DB_CONFIG = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'admin123',
    'database': 'shop',
    'port': 3308
}


def load_csv(file):
    with open(os.path.join(DATA_DIR, file), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def get_record_count():
    """Określa liczbę rekordów na podstawie pliku users.csv"""
    try:
        with open(os.path.join(DATA_DIR, "users.csv"), 'r', encoding='utf-8') as f:
            # Odejmujemy 1 dla pominięcia nagłówka
            return sum(1 for _ in f) - 1
    except FileNotFoundError:
        return 0


def setup_results_dir():
    """Tworzy katalog wyników na podstawie liczby rekordów"""
    record_count = get_record_count()
    result_dir = os.path.join(RESULTS_DIR, f"records_{record_count}")
    os.makedirs(result_dir, exist_ok=True)
    return result_dir


def measure_time(func):
    start = time.time()
    func()
    return time.time() - start


def sample_values(data, field, count):
    if count == 0:
        return []
    # Ograniczamy liczbę próbek do maximum 1000 lub 10% rekordów dla dużych zbiorów danych
    sample_count = min(count, len(data), max(1000, int(len(data) * 0.1)))
    return [item[field] for item in random.sample(data, sample_count)]


def log_result(result_dir, operation, entity, total_time, count):
    avg_time = total_time / count if count else 0
    result_file = os.path.join(result_dir, "mysql_results.csv")
    
    # Sprawdź czy plik istnieje, jeśli nie - utwórz z nagłówkiem
    file_exists = os.path.isfile(result_file)
    with open(result_file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["operation", "database", "entity", "total_time", "avg_time", "record_count"])
        writer.writerow([
            operation, "mysql", entity,
            round(total_time, 4), round(avg_time, 6), count
        ])


def connect():
    return mysql.connector.connect(**DB_CONFIG)


def clear_tables(cursor):
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    for table in ["order_items", "reviews", "orders", "products", "users"]:
        cursor.execute(f"DELETE FROM {table};")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")


def insert_all(cursor, table, data, columns):
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    values = [tuple(row[col] for col in columns) for row in data]
    cursor.executemany(sql, values)


def test_insert(cursor, result_dir, users, products, orders, order_items, reviews):
    print("📝 INSERT...")
    log_result(result_dir, "insert", "users", measure_time(lambda: insert_all(cursor, "users", users, users[0].keys())), len(users))
    log_result(result_dir, "insert", "products", measure_time(lambda: insert_all(cursor, "products", products, products[0].keys())), len(products))
    log_result(result_dir, "insert", "orders", measure_time(lambda: insert_all(cursor, "orders", orders, orders[0].keys())), len(orders))
    log_result(result_dir, "insert", "order_items", measure_time(lambda: insert_all(cursor, "order_items", order_items, order_items[0].keys())), len(order_items))
    log_result(result_dir, "insert", "reviews", measure_time(lambda: insert_all(cursor, "reviews", reviews, reviews[0].keys())), len(reviews))


def test_read(cursor, result_dir, users, products, orders, reviews):
    print("🔍 READ...")
    # Ograniczamy liczbę próbek dla dużych zbiorów danych
    record_count = get_record_count()
    sample_size = min(record_count // 10, 1000)  # 10% lub max 1000
    
    u_count = min(len(users) // 10, sample_size)
    p_count = min(len(products) // 10, sample_size)
    o_count = min(len(orders) // 10, sample_size)
    r_count = min(len(reviews) // 10, sample_size)
    
    log_result(result_dir, "read", "users", measure_time(lambda: [
        (cursor.execute("SELECT * FROM users WHERE email = %s", (email,)), cursor.fetchall())
        for email in sample_values(users, "email", u_count)
    ]), u_count)
    log_result(result_dir, "read", "products", measure_time(lambda: [
        (cursor.execute("SELECT * FROM products WHERE name = %s", (name,)), cursor.fetchall())
        for name in sample_values(products, "name", p_count)
    ]), p_count)
    log_result(result_dir, "read", "orders", measure_time(lambda: [
        (cursor.execute("SELECT * FROM orders WHERE user_id = %s", (uid,)), cursor.fetchall())
        for uid in sample_values(orders, "user_id", o_count)
    ]), o_count)
    log_result(result_dir, "read", "reviews", measure_time(lambda: [
        (cursor.execute("SELECT * FROM reviews WHERE product_id = %s", (pid,)), cursor.fetchall())
        for pid in sample_values(reviews, "product_id", r_count)
    ]), r_count)


def test_update(cursor, result_dir, users, products, orders, reviews):
    print("✏️ UPDATE...")
    # Ograniczamy liczbę próbek dla dużych zbiorów danych
    record_count = get_record_count()
    sample_size = min(record_count // 10, 1000)  # 10% lub max 1000
    
    u_count = min(len(users) // 10, sample_size)
    p_count = min(len(products) // 10, sample_size)
    o_count = min(len(orders) // 10, sample_size)
    r_count = min(len(reviews) // 10, sample_size)
    
    log_result(result_dir, "update", "users", measure_time(lambda: [
        cursor.execute("UPDATE users SET registration_date = %s WHERE email = %s",
                       (datetime.now(timezone.utc), email))
        for email in sample_values(users, "email", u_count)
    ]), u_count)
    log_result(result_dir, "update", "products", measure_time(lambda: [
        cursor.execute("UPDATE products SET stock = stock + 1 WHERE name = %s", (name,))
        for name in sample_values(products, "name", p_count)
    ]), p_count)
    log_result(result_dir, "update", "orders", measure_time(lambda: [
        cursor.execute("UPDATE orders SET status = 'Completed' WHERE id = %s", (oid,))
        for oid in sample_values(orders, "id", o_count)
    ]), o_count)
    log_result(result_dir, "update", "reviews", measure_time(lambda: [
        cursor.execute("UPDATE reviews SET rating = 5 WHERE id = %s", (rid,))
        for rid in sample_values(reviews, "id", r_count)
    ]), r_count)


def test_delete(cursor, result_dir, users, products, orders, reviews, order_items):
    print("🗑️ DELETE...")
    # Ograniczamy liczbę próbek dla dużych zbiorów danych
    record_count = get_record_count()
    sample_size = min(record_count // 20, 500)  # 5% lub max 500
    
    u_count = min(len(users) // 20, sample_size)
    p_count = min(len(products) // 20, sample_size)
    o_count = min(len(orders) // 20, sample_size)
    r_count = min(len(reviews) // 20, sample_size)
    oi_count = min(len(order_items) // 20, sample_size)
    
    log_result(result_dir, "delete", "reviews", measure_time(lambda: [
        cursor.execute("DELETE FROM reviews WHERE id = %s", (rid,))
        for rid in sample_values(reviews, "id", r_count)
    ]), r_count)

    log_result(result_dir, "delete", "order_items", measure_time(lambda: [
        cursor.execute("DELETE FROM order_items WHERE order_id = %s", (oid,))
        for oid in sample_values(orders, "id", oi_count)
    ]), oi_count)

    log_result(result_dir, "delete", "orders", measure_time(lambda: [
        cursor.execute("DELETE FROM orders WHERE id = %s", (oid,))
        for oid in sample_values(orders, "id", o_count)
    ]), o_count)

    log_result(result_dir, "delete", "products", measure_time(lambda: [
        cursor.execute("DELETE FROM products WHERE name = %s", (name,))
        for name in sample_values(products, "name", p_count)
    ]), p_count)

    log_result(result_dir, "delete", "users", measure_time(lambda: [
        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        for email in sample_values(users, "email", u_count)
    ]), u_count)


def test_complex_queries(cursor, result_dir):
    print("🔍 COMPLEX QUERIES...")
    
    # 1. Najpopularniejsze produkty
    log_result(result_dir, "complex", "popular_products", measure_time(lambda: [
        cursor.execute('''
            SELECT p.id, p.name, COUNT(oi.product_id) as order_count
            FROM products p
            JOIN order_items oi ON p.id = oi.product_id
            GROUP BY p.id, p.name
            ORDER BY order_count DESC
            LIMIT 10
        '''), cursor.fetchall()
    ]), 1)

    
    # 2. Średnia ocena produktów
    log_result(result_dir, "complex", "avg_product_rating", measure_time(lambda: [
        cursor.execute('''
            SELECT p.id, p.name, AVG(r.rating) as avg_rating, COUNT(r.id) as review_count
            FROM products p
            JOIN reviews r ON p.id = r.product_id
            GROUP BY p.id, p.name
            HAVING COUNT(r.id) >= 5
            ORDER BY avg_rating DESC
            LIMIT 10
        '''), cursor.fetchall()
    ]), 1)

    
    # 3. Analiza wartości zamówień klientów
    log_result(result_dir, "complex", "customer_spending", measure_time(lambda: [
        cursor.execute('''
            SELECT u.id, u.email, SUM(oi.price * oi.quantity) as total_spent
            FROM users u
            JOIN orders o ON u.id = o.user_id
            JOIN order_items oi ON o.id = oi.order_id
            GROUP BY u.id, u.email
            ORDER BY total_spent DESC
            LIMIT 20
        '''),
        cursor.fetchall()
    ]), 1)
    
    # 4. Wyszukiwanie produktów z filtrowaniem
    log_result(result_dir, "complex", "product_search", measure_time(lambda: [
        cursor.execute('''
            SELECT p.id, p.name, p.price
            FROM products p
            WHERE (p.name LIKE '%laptop%' OR p.description LIKE '%laptop%')
            AND p.price BETWEEN 100 AND 500
            AND p.stock > 0
            ORDER BY p.price ASC
        '''),
        cursor.fetchall()
    ]), 1)
    
    # 5. Dashboard sprzedażowy
    log_result(result_dir, "complex", "sales_dashboard", measure_time(lambda: [
        cursor.execute('''
            SELECT DATE(o.order_date) as date, 
                  COUNT(o.id) as order_count, 
                  SUM(oi.price * oi.quantity) as revenue
            FROM orders o
            JOIN order_items oi ON o.id = oi.order_id
            WHERE o.order_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY DATE(o.order_date)
            ORDER BY date
        '''),
        cursor.fetchall()
    ]), 1)
    
    # 6. Rekomendacje produktów
    log_result(result_dir, "complex", "product_recommendations", measure_time(lambda: [
        cursor.execute('''
            SELECT oi1.product_id, oi2.product_id, COUNT(*) as frequency
            FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id = oi2.order_id AND oi1.product_id < oi2.product_id
            GROUP BY oi1.product_id, oi2.product_id
            ORDER BY frequency DESC
            LIMIT 10
        '''), cursor.fetchall()
    ]), 1)


def main():
    result_dir = setup_results_dir()
    
    print("🔄 Wczytywanie danych CSV...")
    users = load_csv("users.csv")
    products = load_csv("products.csv")
    orders = load_csv("orders.csv")
    order_items = load_csv("order_items.csv")
    reviews = load_csv("reviews.csv")

    print("🔌 Łączenie z bazą MySQL...")
    conn = connect()
    cursor = conn.cursor()

    clear_tables(cursor)
    test_insert(cursor, result_dir, users, products, orders, order_items, reviews)
    conn.commit()

    test_read(cursor, result_dir, users, products, orders, reviews)
    test_update(cursor, result_dir, users, products, orders, reviews)
    
    # Testy złożonych zapytań
    test_complex_queries(cursor, result_dir)
    
    test_delete(cursor, result_dir, users, products, orders, reviews, order_items)
    conn.commit()

    cursor.close()
    conn.close()
    print(f"✅ Testy zakończone. Wyniki zapisane w '{result_dir}/mysql_results.csv'")


if __name__ == "__main__":
    main()