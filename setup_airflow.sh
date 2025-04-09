#!/bin/bash

# Create necessary directories
mkdir -p ./dags ./logs ./plugins ./data

# Copy your DAG file to the dags directory
cp airflow/dags/reddit_scraper_dag.py ./dags/

# Copy your source code to the dags directory
mkdir -p ./dags/src
cp -r src/* ./dags/src/

# Set the correct permissions
echo -e "\nSetting the correct permissions for Airflow..."
mkdir -p ./sources/{logs,dags,plugins}
chmod -R 777 ./sources

# Initialize the database
echo -e "\nInitializing the database..."
docker-compose up airflow-init

# Start the services
echo -e "\nStarting Airflow services..."
docker-compose up -d

echo -e "\nAirflow is now running!"
echo -e "Web UI: http://localhost:8080"
echo -e "Default credentials: airflow / airflow"
echo -e "\nTo stop Airflow, run: docker-compose down" 