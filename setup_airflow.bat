@echo off
echo Setting up Airflow with Docker...

REM Create necessary directories
mkdir dags 2>nul
mkdir logs 2>nul
mkdir plugins 2>nul
mkdir data 2>nul

REM Copy your DAG file to the dags directory
echo Copying DAG file...
copy airflow\dags\reddit_scraper_dag.py dags\

REM Copy your source code to the dags directory
echo Copying source code...
mkdir dags\src 2>nul
xcopy /E /I src dags\src

REM Set the correct permissions
echo Setting permissions...
mkdir sources\logs 2>nul
mkdir sources\dags 2>nul
mkdir sources\plugins 2>nul

REM Check if docker-compose or docker compose is available
where docker-compose >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    set DOCKER_COMPOSE_CMD=docker-compose
) else (
    where docker >nul 2>nul
    if %ERRORLEVEL% EQU 0 (
        set DOCKER_COMPOSE_CMD=docker compose
    ) else (
        echo Error: Neither docker-compose nor docker compose is available.
        echo Please install Docker Desktop or Docker Compose.
        exit /b 1
    )
)

REM Initialize the database
echo Initializing the database...
%DOCKER_COMPOSE_CMD% up airflow-init

REM Start the services
echo Starting Airflow services...
%DOCKER_COMPOSE_CMD% up -d

echo.
echo Airflow is now running!
echo Web UI: http://localhost:8080
echo Default credentials: airflow / airflow
echo.
echo To stop Airflow, run: %DOCKER_COMPOSE_CMD% down 