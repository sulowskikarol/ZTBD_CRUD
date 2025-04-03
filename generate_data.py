import csv
import os
import random
import string
from datetime import datetime, timedelta
from faker import Faker # type: ignore

fake = Faker("pl_PL")
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def generate_users(n):
    users = []
    for i in range(1, n + 1):
        users.append({
            "id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"user{i}@example.com",
            "password": ''.join(random.choices(string.ascii_letters + string.digits, k=12)),
            "registration_date": fake.date_time_this_year().isoformat()
        })
    return users


def generate_products(n):
    products = []
    for i in range(1, n + 1):
        products.append({
            "id": i,
            "name": f"{fake.color_name()} {random.choice(['Laptop', 'Telefon', 'Monitor', 'Tablet', 'Kamera'])}",
            "description": fake.sentence(nb_words=6),
            "price": round(random.uniform(10.0, 5000.0), 2),
            "stock": random.randint(1, 100)
        })
    return products


def generate_orders(n, user_ids, product_ids):
    orders = []
    order_items = []
    for i in range(1, n + 1):
        uid = random.choice(user_ids)
        order_id = i
        orders.append({
            "id": order_id,
            "user_id": uid,
            "order_date": fake.date_time_this_year().isoformat(),
            "status": random.choice(["Pending", "Shipped", "Cancelled", "Completed"])
        })

        num_items = random.randint(1, 5)
        for _ in range(num_items):
            pid = random.choice(product_ids)
            qty = random.randint(1, 3)
            price = round(random.uniform(10.0, 5000.0), 2)
            order_items.append({
                "id": len(order_items) + 1,
                "order_id": order_id,
                "product_id": pid,
                "quantity": qty,
                "price": price
            })

    return orders, order_items


def generate_reviews(n, user_ids, product_ids):
    reviews = []
    for i in range(1, n + 1):
        reviews.append({
            "id": i,
            "product_id": random.choice(product_ids),
            "user_id": random.choice(user_ids),
            "rating": random.randint(1, 5),
            "comment": fake.sentence(),
            "created_at": fake.date_time_this_year().isoformat()
        })
    return reviews


def write_csv(filename, fieldnames, data):
    with open(os.path.join(DATA_DIR, filename), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def main(count=1000):
    print(f"🔄 Generowanie {count} rekordów dla każdej kategorii...")

    users = generate_users(count)
    products = generate_products(count)
    orders, order_items = generate_orders(count, [u["id"] for u in users], [p["id"] for p in products])
    reviews = generate_reviews(count, [u["id"] for u in users], [p["id"] for p in products])

    write_csv("users.csv", users[0].keys(), users)
    write_csv("products.csv", products[0].keys(), products)
    write_csv("orders.csv", orders[0].keys(), orders)
    write_csv("order_items.csv", order_items[0].keys(), order_items)
    write_csv("reviews.csv", reviews[0].keys(), reviews)

    print("✅ Dane testowe wygenerowane i zapisane w folderze 'data/'.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generator danych testowych dla systemu e-commerce")
    parser.add_argument("--count", type=int, default=1000, help="Liczba rekordów do wygenerowania dla każdej kategorii")
    args = parser.parse_args()

    main(args.count)
