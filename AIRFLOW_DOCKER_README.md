# Running Airflow with Docker

This guide explains how to run Apache Airflow using Docker for your skincare ratings project.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Setup Instructions

### For Windows Users

1. Open Command Prompt or PowerShell
2. Navigate to your project directory
3. Run the setup script:
   ```
   setup_airflow.bat
   ```

### For Linux/Mac Users

1. Open Terminal
2. Navigate to your project directory
3. Make the setup script executable:
   ```
   chmod +x setup_airflow.sh
   ```
4. Run the setup script:
   ```
   ./setup_airflow.sh
   ```

## Accessing the Airflow UI

Once the setup is complete, you can access the Airflow web interface at:
```
http://localhost:8080
```

Default credentials:
- Username: `airflow`
- Password: `airflow`

## Managing Your Airflow Instance

### Starting Airflow

```
docker-compose up -d
```

### Stopping Airflow

```
docker-compose down
```

### Viewing Logs

```
docker-compose logs -f
```

### Restarting Services

```
docker-compose restart
```

## Moving to Cloud (Optional)

If you want to move your Airflow setup to the cloud, you have several options:

1. **AWS**: Use AWS ECS or EKS to run your Docker containers
2. **Google Cloud**: Use Google Cloud Run or GKE
3. **Azure**: Use Azure Container Instances or AKS
4. **DigitalOcean**: Use DigitalOcean Kubernetes Service

The Docker setup we've created can be easily migrated to any of these cloud providers.

## Troubleshooting

### Common Issues

1. **Port conflicts**: If port 8080 is already in use, modify the port mapping in `docker-compose.yml`
2. **Permission issues**: Ensure the setup script has the correct permissions
3. **DAG not appearing**: Check the logs for any errors in your DAG file

### Getting Help

If you encounter issues, check the Airflow documentation or run:
```
docker-compose logs -f airflow-webserver
``` 