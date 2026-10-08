from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
with DAG(
    dag_id='prueba_celery_rabbitmq',
    default_args=default_args,
    description='Un DAG simple para probar CeleryExecutor y RabbitMQ',
    schedule=None, # <-- ¡Aquí está el cambio! En Airflow 3+ se usa "schedule" en lugar de "schedule_interval"
    catchup=False,
    tags=['prueba', 'celery'],
) as dag:

    # Tarea 1: Imprime un mensaje rápido en bash
    start_task = BashOperator(
        task_id='iniciar_prueba',
        bash_command='echo "Iniciando la prueba de Celery y RabbitMQ..."'
    )

    # Tarea 2: Una función de Python que simula un trabajo pesado (espera 15 segundos)
    def simular_trabajo_pesado():
        print("Iniciando procesamiento de datos...")
        for i in range(1, 6):
            print(f"Procesando lote {i}/5...")
            time.sleep(3) # Pausa de 3 segundos por iteración (Total 15s)
        print("¡Procesamiento completado con éxito!")
        return "Trabajo finalizado"

    heavy_task = PythonOperator(
        task_id='simulacion_trabajo_pesado',
        python_callable=simular_trabajo_pesado
    )

    # Tarea 3: Finaliza con otro mensaje bash
    end_task = BashOperator(
        task_id='finalizar_prueba',
        bash_command='echo "¡Todas las tareas se han ejecutado correctamente en el worker!"'
    )

    # Definimos el orden de ejecución (Dependencias)
    start_task >> heavy_task >> end_task