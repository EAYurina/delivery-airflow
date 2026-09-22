\# 🚀 Delivery Time Prediction ML Pipeline

\*\*Производственный ML-пайплайн для батч-скоринга времени доставки с использованием Apache Airflow\*\*
\---

\## 📊 Обзор проекта

Автоматизированный пайплайн для пакетного предсказания времени доставки заказов. Модель CatBoost применяется к новым данным каждые 6 часов, результаты сохраняются для дальнейшего использования.

\---

\## 🎯 Ключевые возможности

\- \*\*Батч-скоринг\*\* — автоматическая обработка новых заказов каждые 6 часов
\- \*\*Отказоустойчивость\*\* — DAG не падает при отсутствии файлов или некорректных данных

\- \*\*Контейнеризация\*\* — все сервисы изолированы в Docker
\- \*\*Мониторинг\*\* — Web UI для отслеживания выполнения и просмотра логов

\---

\## 📊 Метрики модели

| Метрика | Значение |
|---------|----------|
| \*\*Модель\*\* | CatBoost |
| \*\*Val MAE\*\* | 11.677 |
| \*\*Val R²\*\* | 0.7125 |
| \*\*OOT MAE\*\* | 8.972 |
| \*\*OOT R²\*\* | 0.7620 |

\---

\## 📁 Структура проекта

delivery-airflow/

├── dags/
│ └── delivery\_scoring.py # DAG для батч-скоринга
├── data/
│ ├── input/ # Входные CSV с заказами
│ └── output/ # Результаты с предсказаниями
├── models/
│ ├── model.joblib # Обученная модель CatBoost
│ └── preprocessor.joblib # Препроцессор
├── logs/ # Логи Airflow
├── plugins/ # Плагины
├── config/ # Конфигурация
├── docker-compose.yaml # Конфигурация сервисов
├── requirements.txt # Зависимости
└── README.md
\---

\## 🔧 Установка и запуск

\### Требования
\- Docker + Docker Compose
\- Python 3.10+
\- Git

---

## 🔧 1. Подготовка окружения

### 🐧 Linux / 🍎 macOS

```bash
# 1. Создаём .env файл с UID пользователя (нужно для прав в Docker)
echo "AIRFLOW_UID=$(id -u)" > .env

# 2. Создаём виртуальное окружение
python3 -m venv venv

# 3. Активируем окружение
source venv/bin/activate

# 4. Устанавливаем зависимости
pip install -r requirements.txt
```

---

### 🪟 Windows (PowerShell)

```powershell
# 1. Создаём .env файл (AIRFLOW_UID не нужен на Windows)
# Использовать произвольный ID для совместимости
"AIRFLOW_UID=50000" | Out-File -FilePath .env -Encoding ASCII
```

> 📌 **Важно для Windows:**
> `AIRFLOW_UID` используется для Linux/macOS, на Windows Docker Desktop сам управляет правами.

```powershell
# 2. Создаём виртуальное окружение
python -m venv venv

# 3. Активируем окружение
venv\Scripts\Activate.ps1

# 4. Устанавливаем зависимости
pip install -r requirements.txt
```

> ❗ Если PowerShell ругается на политику выполнения:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## 🐳 2. Запуск Airflow для батч-скоринга

### Инициализация Airflow (один раз)

```bash
docker compose up airflow-init
```

### Запуск всех сервисов

```bash
docker compose up -d
```

### Проверка статуса

```bash
docker compose ps
```

---

\## 🌐 Работа с Airflow UI

\* \*\*Airflow UI\*\*: \[http://localhost:8080](http://localhost:8080)
\* \*\*Логин\*\*: `airflow`
\* \*\*Пароль\*\*: `airflow`
\* \*\*DAG\*\*: `delivery\_scoring`

\### 🎯 Как запустить батч-скоринг:

1\. Откройте Airflow UI
2\. Найдите DAG `delivery\_scoring`
3\. Нажмите ▶️ кнопку "Trigger DAG"
4\. Следите за выполнением в Graph View