import subprocess
import time
import os
import sys

MAX_DURATION_SECONDS = 20 * 60  # 20 minut
count = 20000
step = 20000

# Ścieżka do katalogu z venv
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(SCRIPT_DIR, "venv", "bin", "python3")  # dla Linux/MacOS

if not os.path.isfile(VENV_PYTHON):
    print("❌ Nie znaleziono pliku venv/bin/python3. Upewnij się, że venv jest poprawnie utworzony.")
    sys.exit(1)

def run_command(cmd):
    print(f"\n▶️  Running: {cmd}")
    start = time.time()
    result = subprocess.run(cmd, shell=False)
    duration = time.time() - start
    print(f"⏱  Finished in {round(duration / 60, 2)} minutes")
    return result.returncode, duration

while True:
    print(f"\n🔁 Generating and testing for count = {count}")

    # 1. Generowanie danych
    code, gen_time = run_command([VENV_PYTHON, "generate_data.py", "--count", str(count)])
    if code != 0:
        print("❌ Data generation failed.")
        break

    # 2. Testy CRUD
    durations = []
    # for script in ["mysql_crud_test.py", "mariadb_crud_test.py", "postgresql_crud_test.py", "mongo_crud_test.py"]:
    for script in ["mongo_crud_test.py"]:
        code, duration = run_command([VENV_PYTHON, script])
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
