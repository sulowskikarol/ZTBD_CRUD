import csv
import os
import time
import random
import mysql.connector
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = "../../data"
RESULTS_DIR = "../../results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# 🔐 Dane połączenia z MySQL
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'ecommerce_benchmark',
    'port': 3306
}


def load_csv(file):
    with open(os.path.join(DATA_DIR, file), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def measure_time(func):
    start = time.time()
    func()
    return time.time() - start


def sample_values(data, field, count):
    if count == 0:
        return []
    return [item[field] for item in random.sample(data, min(count, len(data)))]


def log_result(operation, entity, total_time, count):
    avg_time = total_time / count if count else 0
    with open(os.path.join(RESULTS_DIR, "mysql_results.csv"), "a", newline="") as f:
        writer = csv.writer(f)
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


def test_insert(cursor, users, products, orders, order_items, reviews):
    print("📝 INSERT...")
    log_result("insert", "users", measure_time(lambda: insert_all(cursor, "users", users, users[0].keys())), len(users))
    log_result("insert", "products", measure_time(lambda: insert_all(cursor, "products", products, products[0].keys())), len(products))
    log_result("insert", "orders", measure_time(lambda: insert_all(cursor, "orders", orders, orders[0].keys())), len(orders))
    log_result("insert", "order_items", measure_time(lambda: insert_all(cursor, "order_items", order_items, order_items[0].keys())), len(order_items))
    log_result("insert", "reviews", measure_time(lambda: insert_all(cursor, "reviews", reviews, reviews[0].keys())), len(reviews))


def test_read(cursor, users, products, orders, reviews):
    print("🔍 READ...")
    u_count, p_count, o_count, r_count = len(users) // 2, len(products) // 2, len(orders) // 2, len(reviews) // 2
    log_result("read", "users", measure_time(lambda: [
        (cursor.execute("SELECT * FROM users WHERE email = %s", (email,)), cursor.fetchall())
        for email in sample_values(users, "email", u_count)
    ]), u_count)
    log_result("read", "products", measure_time(lambda: [
        (cursor.execute("SELECT * FROM products WHERE name = %s", (name,)), cursor.fetchall())
        for name in sample_values(products, "name", p_count)
    ]), p_count)
    log_result("read", "orders", measure_time(lambda: [
        (cursor.execute("SELECT * FROM orders WHERE user_id = %s", (uid,)), cursor.fetchall())
        for uid in sample_values(orders, "user_id", o_count)
    ]), o_count)
    log_result("read", "reviews", measure_time(lambda: [
        (cursor.execute("SELECT * FROM reviews WHERE product_id = %s", (pid,)), cursor.fetchall())
        for pid in sample_values(reviews, "product_id", r_count)
    ]), r_count)


def test_update(cursor, users, products, orders, reviews):
    print("✏️ UPDATE...")
    u_count, p_count, o_count, r_count = len(users) // 2, len(products) // 2, len(orders) // 2, len(reviews) // 2
    log_result("update", "users", measure_time(lambda: [
        cursor.execute("UPDATE users SET registration_date = %s WHERE email = %s",
                       (datetime.now(timezone.utc), email))
        for email in sample_values(users, "email", u_count)
    ]), u_count)
    log_result("update", "products", measure_time(lambda: [
        cursor.execute("UPDATE products SET stock = stock + 1 WHERE name = %s", (name,))
        for name in sample_values(products, "name", p_count)
    ]), p_count)
    log_result("update", "orders", measure_time(lambda: [
        cursor.execute("UPDATE orders SET status = 'Completed' WHERE id = %s", (oid,))
        for oid in sample_values(orders, "id", o_count)
    ]), o_count)
    log_result("update", "reviews", measure_time(lambda: [
        cursor.execute("UPDATE reviews SET rating = 5 WHERE id = %s", (rid,))
        for rid in sample_values(reviews, "id", r_count)
    ]), r_count)


def test_delete(cursor, users, products, orders, reviews):
    print("🗑️ DELETE...")
    u_count, p_count, o_count, r_count = len(users) // 2, len(products) // 2, len(orders) // 2, len(reviews) // 2
    log_result("delete", "users", measure_time(lambda: [
        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        for email in sample_values(users, "email", u_count)
    ]), u_count)
    log_result("delete", "products", measure_time(lambda: [
        cursor.execute("DELETE FROM products WHERE name = %s", (name,))
        for name in sample_values(products, "name", p_count)
    ]), p_count)
    log_result("delete", "orders", measure_time(lambda: [
        cursor.execute("DELETE FROM orders WHERE id = %s", (oid,))
        for oid in sample_values(orders, "id", o_count)
    ]), o_count)
    log_result("delete", "reviews", measure_time(lambda: [
        cursor.execute("DELETE FROM reviews WHERE id = %s", (rid,))
        for rid in sample_values(reviews, "id", r_count)
    ]), r_count)


def main():
    Path(os.path.join(RESULTS_DIR, "mysql_results.csv")).write_text("operation,database,entity,total_time,avg_time,record_count\n")

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
    test_insert(cursor, users, products, orders, order_items, reviews)
    conn.commit()

    test_read(cursor, users, products, orders, reviews)
    test_update(cursor, users, products, orders, reviews)
    test_delete(cursor, users, products, orders, reviews)
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ Testy zakończone. Wyniki zapisane w 'results/mysql_results.csv'")


if __name__ == "__main__":
    main()
