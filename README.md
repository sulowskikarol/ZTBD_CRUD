# E-Commerce Database Benchmarking Suite

This project is a comprehensive benchmarking suite designed to evaluate which **database management system (DBMS)** performs best for a typical **e-commerce workload**. It covers relational and non-relational databases and measures the performance of standard operations on large datasets.

## Project Structure

### Setup & Configuration

* `setup_env.sh`: Shell script that creates a Python virtual environment and installs required dependencies.
* `docker-compose.yml`: Defines and launches all database containers (MySQL, MariaDB, PostgreSQL, MongoDB 4, and MongoDB latest).
* `init_mysql.sql` and `init_postgres.sql`: SQL scripts for initializing schema for MySQL, MariaDB (shared script), and PostgreSQL respectively.

### Data Generation

* `generate_data.py`: Generates randomized data for the benchmark in CSV format. Accepts the `--count` flag to control the size of the dataset.

  * For example, `--count 1000` generates:

    * 1000 users
    * 1000 products
    * 1000 orders, each with 1-5 random `order_items`
    * 1000 reviews
  * MongoDB embeds `order_items` inside each order; relational DBs use a separate table.

### Benchmark Scripts

* `mysql_crud_test.py`
* `mariadb_crud_test.py`
* `postgresql_crud_test.py`
* `mongo_crud_test.py`: Runs the same tests for both MongoDB 4 and MongoDB latest

Each script runs the same test scenarios across different databases:

#### Test Cases

* **INSERT**: Bulk insert of all dataset entities
* **READ**: Fetch by indexed and non-indexed fields (e.g., by `email`, `name`, `id`, `user_id`)
* **UPDATE**: Update random entities by both indexed and non-indexed fields
* **COMPLEX QUERIES**:

  * Most popular products (by order count)
  * Products with highest average rating (min. 5 reviews)
  * Top customers by total spending
  * Product search (e.g. name/description contains "laptop", price in range, stock)
  * Daily sales and revenue for the last 30 days
  * Frequently co-purchased products
  * Comments from users who actually purchased the product
* **DELETE**: Delete random subsets of records

### Automated Benchmark Runner

* `benchmark_runner.py`: Automates the full benchmarking process.

  * Starts from 20,000 records, increments by 20,000
  * Runs all tests (including data generation)
  * Terminates when any single test exceeds **20 minutes**

## Usage `(Linux)`

### 1. Clone the repository

```bash
git clone https://github.com/sulowskikarol/ZTBD_CRUD.git
cd ZTBD_CRUD
```

### 2. Setup environment

```bash
bash setup_env.sh
```

### 3. Launch database containers

```bash
docker-compose up -d
```

### 4. Activate virtual environment

```bash
. venv/bin/activate
```

### 5. Generate test data

```bash
python3 generate_data.py --count `<count>`
```

### 6. Run benchmarks manually

```bash
python3 mysql_crud_test.py
python3 mariadb_crud_test.py
python3 postgresql_crud_test.py
python3 mongo_crud_test.py
```

### 7. Or run automated benchmark loop

```bash
python3 benchmark_runner.py
```

## Output

Results are saved to CSV files under `results/records_<N>/` for each dataset size and database engine.
Each file includes operation name, total time, average time per record, and number of records.

## Goal

This project aims to provide real-world performance insights into how different database engines handle a high-volume e-commerce-like workload, including both CRUD and analytical operations.

---