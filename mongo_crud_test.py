import csv
import os
import random
import time
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_DIR = "data"
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

MONGO_INSTANCES = {
    "mongo4": "mongodb://admin:admin123@localhost:27019/",
    "mongolatest": "mongodb://admin:admin123@localhost:27017/",
}

def load_csv(file):
    with open(os.path.join(DATA_DIR, file), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def get_record_count():
    try:
        with open(os.path.join(DATA_DIR, "users.csv"), 'r', encoding='utf-8') as f:
            return sum(1 for _ in f) - 1
    except FileNotFoundError:
        return 0

def setup_results_dir():
    record_count = get_record_count()
    result_dir = os.path.join(RESULTS_DIR, f"records_{record_count}")
    os.makedirs(result_dir, exist_ok=True)
    return result_dir

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
        order["order_date"] = datetime.fromisoformat(order["order_date"])
        order["status"] = order["status"]

    return orders

def prepare_reviews(reviews):
    for r in reviews:
        r["id"] = int(r["id"])
        r["user_id"] = int(r["user_id"])
        r["product_id"] = int(r["product_id"])
        r["rating"] = int(r["rating"])
    return reviews

def prepare_users(users):
    for u in users:
        u["id"] = int(u["id"])
    return users

def prepare_products(products):
    for p in products:
        p["id"] = int(p["id"])
        p["price"] = float(p["price"])
        p["stock"] = int(p["stock"])
    return products


def measure_time(func):
    start = time.perf_counter()
    try:
        func()
    except Exception as e:
        print(f"❌ Błąd w func: {e}")
        return 0.0
    end = time.perf_counter()
    return max(0.0, end - start)


def insert_data(collection, data):
    return lambda: db[collection].insert_many(data)


def read_data(collection, field, values):
    return lambda: [list(db[collection].find({field: val})) for val in values]


def update_data(collection, field, values):
    return lambda: [db[collection].update_many({field: val}, {"$set": {"updated_at": datetime.now(timezone.utc)}}) for val in values]


def delete_data(collection, field, values):
    return lambda: [db[collection].delete_many({field: val}) for val in values]


def sample_values(data, field, count, cast_fn=None):
    if count == 0:
        return []
    sample = random.sample(data, min(count, len(data)))
    return [cast_fn(item[field]) if cast_fn else item[field] for item in sample]


def log_result(operation, db_version, entity, total_time, count):
    avg_time = total_time / count if count else 0
    results_path = os.path.join(result_dir, f"{db_version}_results.csv")
    with open(results_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            operation,
            db_version,
            entity,
            round(total_time, 4),
            round(avg_time, 6),
            count
        ])


def clear_collections():
    print("🧹 Czyszczenie kolekcji MongoDB...")

    db.users.drop()
    db.products.drop()
    db.orders.drop()
    db.reviews.drop()

def ensure_indexes():
    print("📌 Tworzenie indeksów...")

    db.users.create_index("email")

    db.products.create_index("id")
    db.products.create_index("name")
    db.products.create_index("price")
    db.products.create_index("stock")

    db.orders.create_index("id")
    db.orders.create_index("user_id")
    db.orders.create_index("order_date")

    db.reviews.create_index("id")
    db.reviews.create_index("product_id")
    db.reviews.create_index("user_id")
    db.reviews.create_index("rating")


def test_insert(users, products, orders, reviews, db_version):
    print("📝 INSERT...")
    log_result("insert", db_version, "users", measure_time(insert_data("users", users)), len(users))
    log_result("insert", db_version, "products", measure_time(insert_data("products", products)), len(products))
    log_result("insert", db_version, "orders", measure_time(insert_data("orders", orders)), len(orders))
    log_result("insert", db_version, "reviews", measure_time(insert_data("reviews", reviews)), len(reviews))


def test_read(users, products, orders, reviews, db_version):
    print("🔍 READ...")
    log_result("read", db_version, "users", measure_time(read_data("users", "email", sample_values(users, "email", 1000))), 1000)
    log_result("read", db_version, "products", measure_time(read_data("products", "name", sample_values(products, "name", 1000))), 1000)
    log_result("read", db_version, "products_by_id", measure_time(read_data("products", "id", sample_values(products, "id", 1000, int))), 1000)
    log_result("read", db_version, "orders", measure_time(read_data("orders", "user_id", sample_values(orders, "user_id", 1000, int))), 1000)
    log_result("read", db_version, "reviews", measure_time(read_data("reviews", "product_id", sample_values(reviews, "product_id", 1000, int))), 1000)


def test_update(users, products, orders, reviews, db_version):
    print("✏️ UPDATE...")
    log_result("update", db_version, "users", measure_time(update_data("users", "email", sample_values(users, "email", 1000))), 1000)
    log_result("update", db_version, "products", measure_time(update_data("products", "name", sample_values(products, "name", 1000))), 1000)
    log_result("update", db_version, "products_by_id", measure_time(update_data("products", "id", sample_values(products, "id", 1000, int))), 1000)
    log_result("update", db_version, "orders", measure_time(update_data("orders", "id", sample_values(orders, "id", 1000, int))), 1000)
    log_result("update", db_version, "reviews", measure_time(update_data("reviews", "id", sample_values(reviews, "id", 1000, int))), 1000)

def test_complex_queries(db_version):
    print("🔍 COMPLEX QUERIES...")

    # 1. Najpopularniejsze produkty
    log_result("complex", db_version, "popular_products", measure_time(lambda:
        list(db.orders.aggregate([
            {"$unwind": "$items"},
            {"$group": {"_id": "$items.product_id", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}},
            {"$limit": 10}
        ]))
    ), 1)

    # 2. Średnia ocena produktów
    log_result("complex", db_version, "avg_product_rating", measure_time(lambda:
        list(db.reviews.aggregate([
            {"$group": {"_id": "$product_id", "avg_rating": {"$avg": "$rating"}, "review_count": {"$sum": 1}}},
            {"$match": {"review_count": {"$gte": 5}}},
            {"$sort": {"avg_rating": -1}},
            {"$limit": 10}
        ]))
    ), 1)

    # 3. Użytkownicy, którzy wydali najwięcej
    log_result("complex", db_version, "customer_spending", measure_time(lambda:
        list(db.orders.aggregate([
            {"$unwind": "$items"},
            {"$group": {"_id": "$user_id", "total_spent": {"$sum": {"$multiply": ["$items.price", "$items.quantity"]}}}},
            {"$sort": {"total_spent": -1}},
            {"$limit": 20}
        ]))
    ), 1)

    # 4. Wyświetlenie produktów, które zawierają "laptop" w nazwie lub w opisie
    log_result("complex", db_version, "product_search", measure_time(lambda:
        list(db.products.find({
            "$and": [
                {"$or": [
                    {"name": {"$regex": "laptop", "$options": "i"}},
                    {"description": {"$regex": "laptop", "$options": "i"}}
                ]},
                {"price": {"$gte": 100, "$lte": 500}},
                {"stock": {"$gt": 0}}
            ]
        }).sort("price", 1))
    ), 1)

    # 5. Dzienny przychód z ostatnich 30 dni
    log_result("complex", db_version, "sales_dashboard", measure_time(lambda:
        list(db.orders.aggregate([
            {"$match": {"order_date": {"$gte": datetime.now(timezone.utc) - timedelta(days=30)}}},
            {"$unwind": "$items"},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$order_date"}},
                "order_count": {"$sum": 1},
                "revenue": {"$sum": {"$multiply": ["$items.price", "$items.quantity"]}}
            }},
            {"$sort": {"_id": 1}}
        ]))
    ), 1)

    # 6. Produkty najczęściej kupowane razem
    log_result("complex", db_version, "product_recommendations", measure_time(lambda:
        list(db.orders.aggregate([
            {"$unwind": "$items"},
            {"$group": {
                "_id": "$id",
                "products": {"$addToSet": "$items.product_id"}
            }},
            {"$unwind": "$products"},
            {"$group": {
                "_id": "$products",
                "co_purchased": {"$addToSet": "$products"}
            }},
            {"$project": {
                "_id": 1,
                "co_purchased": 1,
                "count": {"$size": "$co_purchased"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]))
    ), 1)

    # 7. Wyświetlenie komentarzy napisanych przez użytkowników z potwierdzonym zakupem
    # Pominięte przy testach na dużym rozmiarze danych ze względu na kosztowne przeszukiwanie zagnieżdżeń
    log_result("complex", db_version, "join_with_comments", measure_time(lambda:
        list(db.reviews.aggregate([
            {"$lookup": {
                "from": "orders",
                "let": {"user_id": "$user_id", "product_id": "$product_id"},
                "pipeline": [
                    {"$match": {"$expr": {
                        "$and": [
                            {"$eq": ["$user_id", "$$user_id"]},
                            {"$in": ["$$product_id", "$items.product_id"]}
                        ]
                    }}
                    }
                ],
                "as": "verified_purchase"
            }},
            {"$match": {"verified_purchase.0": {"$exists": True}, "comment": {"$ne": None}}},
            {"$limit": 100}
        ]))
    ), 1)

def test_delete(users, products, orders, reviews, db_version):
    print("🗑️ DELETE...")
    log_result("delete", db_version, "users", measure_time(delete_data("users", "email", sample_values(users, "email", 500))), 500)
    log_result("delete", db_version, "products", measure_time(delete_data("products", "name", sample_values(products, "name", 500))), 500)
    log_result("delete", db_version, "products_by_id", measure_time(delete_data("products", "id", sample_values(products, "id", 500, int))), 500)
    log_result("delete", db_version, "orders", measure_time(delete_data("orders", "id", sample_values(orders, "id", 500, int))), 500)
    log_result("delete", db_version, "reviews", measure_time(delete_data("reviews", "id", sample_values(reviews, "id", 500, int))), 500)


def run_benchmark(db_version):
    print("🔄 Ładowanie danych z CSV...")
    users = prepare_users(load_csv("users.csv"))
    products = prepare_products(load_csv("products.csv"))
    orders = prepare_orders(load_csv("orders.csv"), load_csv("order_items.csv"))
    reviews = prepare_reviews(load_csv("reviews.csv"))

    clear_collections()
    ensure_indexes()

    print(f"📈 Rozpoczynanie testów dla {db_version}...")
    test_insert(users, products, orders, reviews, db_version)
    test_read(users, products, orders, reviews, db_version)
    test_update(users, products, orders, reviews, db_version)
    test_complex_queries(db_version)
    test_delete(users, products, orders, reviews, db_version)

    print(f"✅ Zakończono testy dla {db_version}")


def main():
    for db_version, uri in MONGO_INSTANCES.items():
        print(f"\n🚀 Uruchamianie testów dla {db_version}")
        global client, db
        client = MongoClient(uri)
        db = client["shop"]

        global result_dir 
        result_dir = setup_results_dir()
        Path(os.path.join(result_dir, f"{db_version}_results.csv")).write_text("operation,database,entity,total_time,avg_time,record_count\n")

        run_benchmark(db_version)


if __name__ == "__main__":
    main()
