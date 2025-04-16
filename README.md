# Data Pipeline for Football Stadiums from Wikipedia using Apache Airflow and PostgreSQL

## Introduction

This project implements an ETL (Extract, Transform, Load) pipeline to collect and process data about football stadiums from Wikipedia. It uses Apache Airflow for workflow orchestration, Docker for containerized deployment, and PostgreSQL as the destination database for storing the processed data. The pipeline extracts stadium data, transforms it into a structured format, and loads it into a PostgreSQL database for further analysis or reporting.

### E-T-L Process

ETL stands for **Extract, Transform, Load**, a key process in data engineering:

- **Extract**: Retrieve raw data from a source, such as Wikipedia pages or API endpoints related to football stadiums.
- **Transform**: Clean, standardize, or enrich the data (e.g., extracting stadium capacity, location, or opening date).
- **Load**: Store the processed data into a destination, such as a file system or database, for analysis.

This project automates the ETL process using Airflow's DAGs (Directed Acyclic Graphs) to manage tasks and scheduling.

## Project Goals

- Extract data about football stadiums from Wikipedia (e.g., stadium names, capacities, locations, and other metadata).
- Transform the data into a structured format suitable for analysis (e.g., CSV, JSON).
- Store the processed data for use in reporting, visualization, or other applications.
- Provide a robust and automated pipeline using Airflow and Docker.


## Project Structure

Below is the structure of the project directory:

- `.idea/`: Configuration files for IntelliJ IDEA (IDE-specific settings, can be ignored).
- `dags/`: Contains Airflow DAG definitions that orchestrate the pipeline workflows for stadium data.
- `data/`: Stores raw and processed stadium data (e.g., extracted Wikipedia pages and transformed outputs).
- `pipelines/`: Custom Python modules or scripts for extracting, transforming, and loading stadium data.
- `script/`: Utility scripts for supporting tasks (e.g., data validation, API calls).
- `docker-compose.yml`: Docker Compose configuration file to set up Airflow and its dependencies.
- `requirements.txt`: Lists Python dependencies required for the project.

## Setup Instructions

Follow these steps to set up and run the project on your local machine.

### 1. Clone the Repository

Clone this repository to your local machine:

```bash
git clone https://github.com/trungit03/Data-Pipeline-With-Apache-Airflow.git
cd Data-Pipeline-With-Apache-Airflow
```

### 2. Install Python Dependencies

Install the required Python packages listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 3. Start Airflow with Docker Compose

Run the following command to start Airflow and its services (e.g., PostgreSQL, webserver, scheduler):

```bash
docker-compose up -d
```

This will pull the necessary Docker images and start the containers in the background.

### 4. Access the Airflow Web Interface

- Open your browser and navigate to `http://localhost:8080`.
- Log in with the default credentials:
  - Username: `airflow`
  - Password: `airflow`
