# Air Quality - ETL Project
Created by group 3:

Alissa Cheng

Alan Segal

Mukhamedzhan Abdullaev

## Project Overview

### Context
This project tracks the current air quality and related data of the largest cities in the world.
### Goal 
- It aims to create business solutions as they relate to population, population density, and air quality in cities selected from a CSV file.
- This data pipeline would be valuable to businesses that prioritize respiratory healthcare, air filtering technology, or real estate brokers who target healthy living
### Data source:
- Air Quality API Extraction: Live Dataset https://api.waqi.info/feed
- CSV file of cities: Static Dataset city_data.csv

## Project Structure

The project is structured as follows:

- **app**: Contains the main pipeline script and associated modules.
- **project.assets**: Includes metadata logging, pipeline logging modules and extract, transform and load functions.
- **project.connectors**: Contains connectors for the Air Quality API and PostgreSQL.
- **project.pipelines**: Holds specific pipeline (e.g., `air_quality`) and the pipeline configuration.
- **app.project_tests**: Houses test data and test scripts for the project.

## Docker Integration

The project is Dockerized for easy deployment. The Dockerfile sets up a Python 3.9 environment, installs dependencies, and runs the main pipeline script.

### Building the Docker Image

docker build -t air-quality-etl:latest .

## Running the Docker Container

docker run --env-file .env air-quality-etl:latest

## AWS Integration

The pipeline is designed to be deployable on Amazon Web Services (AWS). The following AWS services are utilized:

- **ECS (Elastic Container Service):** Manages and orchestrates Docker containers.
- **ECR (Elastic Container Registry):** Stores and manages Docker container images.
- **RDS (Relational Database Service):** Hosts the PostgreSQL database.
- **IAM (Identity and Access Management):** Manages roles and permissions.

## Configuration

Make sure to set the necessary AWS environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`


## Getting Started

### Prerequisites

- Python 3.9
- Docker
- AWS Account

## Installation

### 1.Clone the repository:
git clone https://github.com/your-username/air-quality-etl.git
### 2.Install dependencies:
pip install -r requirements.txt
### 3.Set up environment variables (clone .env.example and rename to .env).
### 4.Run the pipeline locally:
cd app
python -m project.pipelines.air_quality
### 5. Run the pipeline tests locally:
python -m pytest project_tests