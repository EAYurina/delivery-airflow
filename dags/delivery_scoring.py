"""
DAG: delivery_scoring
Описание: Пакетный скоринг новых заказов для предсказания времени доставки.

Pipeline:
  1. Загрузка CSV из data/input/
  2. Очистка данных
  3. Применение ML-модели
  4. Сохранение результата в data/output/

Расписание: каждые 6 часов + ручной запуск.
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import pandas as pd
import joblib
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

BASE_DIR = Path("/opt/airflow")
DATA_INPUT_DIR = BASE_DIR / "data" / "input"
DATA_OUTPUT_DIR = BASE_DIR / "data" / "output"
MODELS_DIR = BASE_DIR / "models"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)


def load_data(**context) -> list:
    """Task 1: Загрузка CSV из data/input/."""
    csv_files = sorted(DATA_INPUT_DIR.glob("*.csv"))
    
    if not csv_files:
        logger.warning("Нет CSV-файлов. DAG пропущен.")
        raise AirflowSkipException("Нет данных")
    
    logger.info(f"Найдено файлов: {len(csv_files)}")
    
    loaded_files = []
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            logger.info(f"   {file.name}: {len(df)} строк")
            loaded_files.append({"name": file.name, "data": df})
        except Exception as e:
            logger.error(f"   Ошибка: {e}")
    
    if not loaded_files:
        raise AirflowSkipException("Нет данных")
    
    return loaded_files


def preprocess(**context) -> list:
    """Task 2: Очистка данных."""
    ti = context["ti"]
    files_data = ti.xcom_pull(task_ids="load_data")
    
    if not files_data:
        raise AirflowSkipException("Нет данных")
    
    processed_files = []
    for item in files_data:
        filename = item["name"]
        df = item["data"]
        
        required_cols = ['order_amount', 'distance_km']
        available_cols = [c for c in required_cols if c in df.columns]
        if available_cols:
            df = df.dropna(subset=available_cols)
        
        if df.empty:
            continue
        
        processed_files.append({"name": filename, "data": df})
        logger.info(f"   {filename}: {len(df)} строк")
    
    if not processed_files:
        raise AirflowSkipException("Все файлы пусты")
    
    return processed_files


def score(**context) -> list:
    """Task 3: Применение ML-модели."""
    ti = context["ti"]
    files_data = ti.xcom_pull(task_ids="preprocess")
    
    if not files_data:
        raise AirflowSkipException("Нет данных")
    
    model_path = MODELS_DIR / "model.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Модель не найдена: {model_path}")
    
    model = joblib.load(model_path)
    logger.info(f"Модель: {type(model).__name__}")
    
    scored_files = []
    for item in files_data:
        filename = item["name"]
        df = item["data"]
        
        try:
            predictions = model.predict(df)
            df["prediction_time_minutes"] = np.round(predictions, 2)
            
            scored_files.append({"name": filename, "data": df})
            logger.info(f"   {filename}: [{predictions.min():.1f}, {predictions.max():.1f}] мин")
        except Exception as e:
            logger.error(f"   {filename}: ошибка — {e}")
    
    if not scored_files:
        raise AirflowSkipException("Скоринг не выполнен")
    
    return scored_files


def save_results(**context) -> None:
    """Task 4: Сохранение результатов."""
    ti = context["ti"]
    files_data = ti.xcom_pull(task_ids="score")
    
    if not files_data:
        raise AirflowSkipException("Нет результатов")
    
    DATA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    for item in files_data:
        original_name = item["name"]
        df = item["data"]
        base_name = original_name.replace(".csv", "")
        output_path = DATA_OUTPUT_DIR / f"{base_name}_predicted.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Сохранён: {output_path} ({len(df)} строк)")


with DAG(
    dag_id="delivery_scoring",
    default_args=DEFAULT_ARGS,
    description="Пакетный скоринг времени доставки",
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["delivery", "ml", "scoring"],
) as dag:
    
    task_load = PythonOperator(task_id="load_data", python_callable=load_data)
    task_preprocess = PythonOperator(task_id="preprocess", python_callable=preprocess)
    task_score = PythonOperator(task_id="score", python_callable=score)
    task_save = PythonOperator(task_id="save_results", python_callable=save_results)
    
    task_load >> task_preprocess >> task_score >> task_save