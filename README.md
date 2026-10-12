# 🚀 Delivery Time Prediction — Airflow Batch Scoring

![Python](https://img.shields.io/badge/python-3.10-blue)
![Airflow](https://img.shields.io/badge/airflow-2.10-red)
![Docker](https://img.shields.io/badge/docker-compose-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**Production-grade ML pipeline для батч-скоринга времени доставки с использованием Apache Airflow**

---

## 📊 Обзор проекта

Автоматизированный пайплайн для пакетного предсказания времени доставки заказов. Модель CatBoost применяется к новым данным каждые 6 часов, результаты сохраняются для дальнейшего использования.

Проект — продолжение [delivery-mas](https://github.com/EAYurina/delivery-mas) (REST API для инференса). Здесь та же модель обёрнута в **оркестрацию через Airflow** для регулярного batch-скоринга.

---

## 🎯 Ключевые возможности

- **Батч-скоринг** — автоматическая обработка новых заказов каждые 6 часов
- **Отказоустойчивость** — DAG не падает при отсутствии файлов или некорректных данных
- **Контейнеризация** — все сервисы изолированы в Docker
- **Мониторинг** — Web UI для отслеживания выполнения и просмотра логов

---

## 📊 Метрики модели

| Метрика | Значение |
|---------|----------|
| **Модель** | CatBoost |
| **Val MAE** | 11.677 |
| **Val R²** | 0.7125 |
| **OOT MAE** | 8.972 |
| **OOT R²** | 0.7620 |

Обучение модели: [delivery-mas/notebook/delivery_time_prediction.ipynb](https://github.com/EAYurina/delivery-mas/blob/main/notebook/delivery_time_prediction.ipynb)

---

## 📁 Структура проекта

```
delivery-airflow/
├── dags/
│   └── delivery_scoring.py      # DAG для батч-скоринга
├── data/
│   ├── input/                   # Входные CSV с заказами
│   └── output/                  # Результаты с предсказаниями
├── models/
│   └── model.joblib             # Обученная модель CatBoost
├── logs/                        # Логи Airflow
├── plugins/                     # Плагины
├── config/                      # Конфигурация
├── Dockerfile                   # Кастомный образ Airflow
├── requirements.txt             # Зависимости
├── docker-compose.yaml          # Конфигурация сервисов
├── .gitignore
└── README.md
```

---

## 🧠 Как это работает

### Модель

Используется та же модель, что в [delivery-mas](https://github.com/EAYurina/delivery-mas):
- **CatBoost** для регрессии
- Категориальные признаки обрабатываются внутри модели
- Артефакт: `models/model.joblib`

### Пайплайн

1. **Триггер** — по расписанию каждые 6 часов (или вручную)
2. **Чтение данных** — CSV из `data/input/`
3. **Препроцессинг** — очистка от пропусков
4. **Инференс** — модель предсказывает время доставки
5. **Сохранение** — результат в `data/output/<имя_входного>_predicted.csv`

### Входные данные

DAG ожидает CSV с заказами в `data/input/` — **34 признака** (числовые + категориальные).

**Пример** (`data/input/new_orders.csv`):

| order_amount | items_count | distance_km | customer_city | vehicle_type | prep_time_avg |
|---|---|---|---|---|---|
| 1540 | 2 | 3.5 | Moscow | car | 15 |
| 800 | 1 | 5.0 | SPB | bike | 20 |
| 2100 | 3 | 1.5 | Kazan | scooter | 10 |

### Выходные данные

Результат сохраняется в `data/output/<имя_входного>_predicted.csv` — **34 входных признака + 1 предсказание** (`prediction_time_minutes`).

Например, `data/input/new_orders.csv` → `data/output/new_orders_predicted.csv`.

**Пример**:

| order_amount | distance_km | vehicle_type | prep_time_avg | prediction_time_minutes |
|---|---|---|---|---|
| 1540 | 3.5 | car | 15 | 65.49 |
| 800 | 5.0 | bike | 20 | 87.32 |
| 2100 | 1.5 | scooter | 10 | 30.19 |

---

## 🔧 Требования

- Docker + Docker Compose
- Python 3.10+
- Git

---

## 🚀 Установка и запуск

### 1. Подготовка окружения

#### 🐧 Linux / 🍎 macOS

```bash
echo "AIRFLOW_UID=$(id -u)" > .env

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

#### 🪟 Windows (PowerShell)

```powershell
"AIRFLOW_UID=50000" | Out-File -FilePath .env -Encoding ASCII

python -m venv venv
venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

> ❗ Если PowerShell ругается на политику выполнения:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

---

### 2. Запуск Airflow

```bash
docker compose up airflow-init
docker compose up -d
docker compose ps
```

---

## 🌐 Работа с Airflow UI

- **Airflow UI:** http://localhost:8080
- **Логин:** `airflow`
- **Пароль:** `airflow`
- **DAG:** `delivery_scoring`

### Как запустить батч-скоринг

1. Открой Airflow UI
2. Найди DAG `delivery_scoring`
3. Нажми ▶️ **Trigger DAG**
4. Следи за выполнением в **Graph View**
5. Результат — в `data/output/`

---

## 🛠️ Используемые технологии

- **Apache Airflow** — оркестрация
- **CatBoost** — модель
- **pandas / scikit-learn** — препроцессинг
- **Docker / Docker Compose** — изоляция сервисов
- **PostgreSQL** — метаданные Airflow
- **Joblib** — сериализация модели

---

## 🚧 Ограничения

- DAG запускается по расписанию (каждые 6 часов), без триггера по событию
- Нет алертов при падении — только логи в Airflow UI
- Нет версионирования моделей — модель одна на все запуски
- Работа с локальной файловой системой, без внешнего хранилища (S3/GCS)

---

## 🎯 Что дальше

- Алерты в Telegram/Slack при падении DAG
- Мониторинг через Prometheus + Grafana
- Версионирование моделей через MLflow
- Переход на облачное хранилище (S3) для входных/выходных данных
- Триггер по событию (новый заказ → сразу скоринг)

---

## 📎 Связанные репозитории

- [delivery-mas](https://github.com/EAYurina/delivery-mas) — FastAPI-сервис для online-инференса
- [delivery-data](https://github.com/EAYurina/delivery-data) — датасет для обучения

---

## 📝 Лицензия

MIT