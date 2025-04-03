import csv
import os
import random
import time
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path

DATA_DIR = "data"
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

client = MongoClient("mongodb://admin:admin123@localhost:27017/")
db = client["shop"]


def load_csv(file):
    with open(os.path.join(DATA_DIR, file), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def prepare_orders(orders, order_items):
    items_by_order = {}
    for item in order_items:
        oid = int(item["order_id"])
        items_by_order.setdefault(oid, []).append({
            "product_id": int(item["product_id"]),
            "quantity": int(item["quantity"]),
            "price": float(item["price"])
        })

    for order in orders:
        order["user_id"] = int(order["user_id"])
        order["id"] = int(order["id"])
        order["items"] = items_by_order.get(order["id"], [])
        order["order_date"] = order["order_date"]
        order["status"] = order["status"]

    return orders


def measure_time(func):
    start = time.time()
    func()
    return time.time() - start


def insert_data(collection, data):
    return lambda: db[collection].insert_many(data)


def read_data(collection, field, values):
    return lambda: [db[collection].find_one({field: val}) for val in values]


def update_data(collection, field, values):
    return lambda: [db[collection].update_one({field: val}, {"$set": {"updated_at": datetime.now()}}) for val in values]


def delete_data(collection, field, values):
    return lambda: [db[collection].delete_one({field: val}) for val in values]


def sample_values(data, field, count):
    if count == 0:
        return []
    return [item[field] for item in random.sample(data, min(count, len(data)))]


def log_result(operation, entity, total_time, count):
    avg_time = total_time / count if count else 0
    with open(os.path.join(RESULTS_DIR, "mongo_results.csv"), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            operation,
            "mongodb",
            entity,
            round(total_time, 4),
            round(avg_time, 6),
            count
        ])


def clear_collections():
    db.users.drop()
    db.products.drop()
    db.orders.drop()
    db.reviews.drop()


def test_insert(users, products, orders, reviews):
    print("📝 INSERT...")
    log_result("insert", "users", measure_time(insert_data("users", users)), len(users))
    log_result("insert", "products", measure_time(insert_data("products", products)), len(products))
    log_result("insert", "orders", measure_time(insert_data("orders", orders)), len(orders))
    log_result("insert", "reviews", measure_time(insert_data("reviews", reviews)), len(reviews))


def test_read(users, products, orders, reviews):
    print("🔍 READ...")
    log_result("read", "users", measure_time(read_data("users", "email", sample_values(users, "email", len(users) // 2))), len(users) // 2)
    log_result("read", "products", measure_time(read_data("products", "name", sample_values(products, "name", len(products) // 2))), len(products) // 2)
    log_result("read", "orders", measure_time(read_data("orders", "user_id", sample_values(orders, "user_id", len(orders) // 2))), len(orders) // 2)
    log_result("read", "reviews", measure_time(read_data("reviews", "product_id", sample_values(reviews, "product_id", len(reviews) // 2))), len(reviews) // 2)


def test_update(users, products, orders, reviews):
    print("✏️ UPDATE...")
    log_result("update", "users", measure_time(update_data("users", "email", sample_values(users, "email", len(users) // 2))), len(users) // 2)
    log_result("update", "products", measure_time(update_data("products", "name", sample_values(products, "name", len(products) // 2))), len(products) // 2)
    log_result("update", "orders", measure_time(update_data("orders", "id", sample_values(orders, "id", len(orders) // 2))), len(orders) // 2)
    log_result("update", "reviews", measure_time(update_data("reviews", "id", sample_values(reviews, "id", len(reviews) // 2))), len(reviews) // 2)


def test_delete(users, products, orders, reviews):
    print("🗑️ DELETE...")
    log_result("delete", "users", measure_time(delete_data("users", "email", sample_values(users, "email", len(users) // 2))), len(users) // 2)
    log_result("delete", "products", measure_time(delete_data("products", "name", sample_values(products, "name", len(products) // 2))), len(products) // 2)
    log_result("delete", "orders", measure_time(delete_data("orders", "id", sample_values(orders, "id", len(orders) // 2))), len(orders) // 2)
    log_result("delete", "reviews", measure_time(delete_data("reviews", "id", sample_values(reviews, "id", len(reviews) // 2))), len(reviews) // 2)


def run_benchmark():
    print("🔄 Ładowanie danych z CSV...")
    users = load_csv("users.csv")
    products = load_csv("products.csv")
    orders = load_csv("orders.csv")
    order_items = load_csv("order_items.csv")
    reviews = load_csv("reviews.csv")

    print("🔗 Łączenie zamówień z ich pozycjami...")
    orders = prepare_orders(orders, order_items)

    print("🧹 Czyszczenie kolekcji MongoDB...")
    clear_collections()

    print("📈 Rozpoczynanie testów...")
    test_insert(users, products, orders, reviews)
    test_read(users, products, orders, reviews)
    test_update(users, products, orders, reviews)
    test_delete(users, products, orders, reviews)

    print("✅ Zakończono. Wyniki zapisane w 'results/mongo_results.csv'")


def main():
    Path(os.path.join(RESULTS_DIR, "mongo_results.csv")).write_text("operation,database,entity,total_time,avg_time,record_count\n")
    run_benchmark()


if __name__ == "__main__":
    main()
