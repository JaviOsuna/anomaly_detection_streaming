# Anomalías en Tiempo Real - Madrid
## Real-Time Anomalies - Madrid

## Description

This project develops a scalable solution based on **Spark Structured Streaming** and **Machine Learning** techniques to detect anomalies in real-time traffic and air quality data in the city of Madrid. The goal is to generate alerts for anomalous situations and provide an interactive visualization for monitoring and decision-making.

---

## Technologies Used

- **Python 3.10**
- **Poetry** (dependency management)
- **Apache Spark** (distributed and streaming processing)
- **Polars** (efficient local data processing)
- **MLflow** (ML experiment tracking)
- **Streamlit** (interactive dashboard)
- **Docker** (reproducible environment)
- **Docker Compose** (service orchestration)
- **Jupyter** (optional, for prototyping and exploration)

---

## Project Structure

```
anomaly_detection_streaming/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── poetry.lock
├── src/
│ └── anomalias_madrid/
│ ├── main.py
│ └── dashboard.py
├── notebooks/
├── tests/
├── data/
└── README.md
```
---

## Getting Started

### 1. Prerequisites

- **Docker** and **Docker Compose** installed on your system.
- (Optional, for local development) **Poetry** and **Python 3.10+** installed.

---

### 2. Clone the Repository

```bash
git clone https://github.com/tu_usuario/anomaly_detection_streaming.git
cd anomaly_detection_streaming
```

### 3. Start Services with Docker Compose

This will build the image and launch both Streamlit and MLflow UI in separate containers.

```bash
docker-compose up --build
```

- **Streamlit** will be available at: http://localhost:8501
- **MLflow UI** will be available at: http://localhost:5000

Press `Ctrl+C` to stop the services, or use:

```bash
docker-compose down
```

### 4. Local Development (optional)

If you prefer to work outside Docker:

1. Install dependencies with Poetry:

```bash
poetry install
```

2. Run the main script:

```bash
poetry run python src/main.py
```

3. Launch Streamlit:

```bash
poetry run streamlit run src/dashboard.py
```

4. Launch MLflow UI:

```bash
poetry run mlflow ui
```

## Service Details

### Streamlit

- The visualization dashboard is located at `src/dashboard.py`.
- Access it at http://localhost:8501 when the service is running.

### MLflow

- The experiment tracking interface is available at http://localhost:5000.
- Experiments are automatically logged from the code.

## Notes on Docker and Networking

- **IMPORTANT**: Ports 8501 (Streamlit) and 5000 (MLflow) are mapped to your local machine.

## Next Steps

1. Search for and analyze real traffic and air quality data sources for Madrid.
2. Design the real-time ingestion and processing pipeline with Spark Structured Streaming.
3. Develop and tune anomaly detection models.
4. Integrate visualization and alerting system in Streamlit.
5. Document and test the entire workflow.

## Contact

For questions, suggestions, or collaboration, open an issue in the repository or contact the author.