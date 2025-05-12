import subprocess
import time

MAX_DURATION_SECONDS = 30 * 60  # 30 minut
count = 100000
step = 20000

def run_command(cmd):
    print(f"\n▶️  Running: {cmd}")
    start = time.time()
    result = subprocess.run(cmd, shell=True)
    duration = time.time() - start
    print(f"⏱  Finished in {round(duration / 60, 2)} minutes")
    return result.returncode, duration

while True:
    print(f"\n🔁 Generating and testing for count = {count}")

    # 1. Generowanie danych
    code, gen_time = run_command(f"python3 generate_data.py --count {count}")
    if code != 0:
        print("❌ Data generation failed.")
        break

    # 2. Testy CRUD
    durations = []
    for script in ["mysql_crud_test.py", "mariadb_crud_test.py", "postgresql_crud_test.py", "mongo_crud_test.py"]:
        code, duration = run_command(f"python3 {script}")
        durations.append(duration)
        if code != 0:
            print(f"❌ Script {script} failed.")
            break

    # 3. Czy którykolwiek test przekroczył limit?
    if any(d >= MAX_DURATION_SECONDS for d in durations):
        print("⛔ At least one test exceeded 20 minutes. Stopping loop.")
        break

    # 4. Zwiększ count i kontynuuj
    count += step
