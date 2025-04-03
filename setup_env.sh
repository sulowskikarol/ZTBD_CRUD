#!/bin/bash

# Sprawdzenie, czy Python3 jest dostępny
if ! which python3 > /dev/null 2>&1; then
    echo "Błąd: Python3 nie jest zainstalowany lub nie jest w PATH."
    exit 1
fi

# Sprawdzenie wersji Pythona
PYTHON_VERSION=$(python3 --version)
echo "Znaleziono: $PYTHON_VERSION"

# Tworzenie wirtualnego środowiska, jeśli nie istnieje
if [ ! -d "venv" ]; then
    echo "Tworzenie wirtualnego środowiska Python..."
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo "Błąd: Nie udało się utworzyć wirtualnego środowiska!"
        exit 1
    fi
    echo "Wirtualne środowisko utworzone."
fi

# Aktywacja venv
if [ -f "venv/bin/activate" ]; then
    echo "Aktywowanie wirtualnego środowiska..."
    . venv/bin/activate
elif [ -f "venv/Scripts/activate" ]; then  # Windows WSL fix
    echo "Aktywowanie wirtualnego środowiska (Windows WSL)..."
    . venv/Scripts/activate
else
    echo "Błąd: Nie znaleziono pliku aktywującego venv!"
    exit 1
fi

# Instalowanie wymaganych bibliotek
echo "Instalowanie wymaganych bibliotek..."
pip install --upgrade pip
pip install mysql-connector-python psycopg2-binary pymysql redis pymongo faker

echo "Środowisko gotowe. Możesz uruchomić testy!"
