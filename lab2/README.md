Запуск

```bash
docker-compose up -d

1. Загрузка данных

Подключиться к PostgreSQL и выполнить:

\i sql/init.sql

Загрузить CSV через DBeaver или COPY.

2. Запуск ETL

docker exec -it spark bash

spark-submit spark_jobs/etl_to_star.py

3. Построение витрин

spark-submit spark_jobs/marts_to_clickhouse.py

4. Проверка

Подключиться к ClickHouse:

SELECT * FROM top_products;