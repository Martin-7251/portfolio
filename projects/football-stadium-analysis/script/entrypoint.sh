#!/bin/bash
set -e

echo "🔄 Starting Airflow entrypoint script..."

# Wait for Postgres to be ready
echo "⏳ Waiting for Postgres..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "✅ Postgres is ready."

# Install Python packages from local wheels
echo "📦 Installing Python packages from requirements.txt..."
pip install --no-index --find-links=/opt/airflow/wheels -r /requirements.txt

# Initialize the database if needed
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@airscholar.com \
    --password admin
fi

# Always run upgrade
airflow db upgrade

# Start webserver
exec airflow webserver
