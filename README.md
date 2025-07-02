# Real-time Anomaly Detection in Traffic and Pollution Data Streams with Spark Structured Streaming

## Project Description

This project aims to develop a real-time anomaly detection system for traffic and pollution data in the city of Madrid, using Apache Spark Structured Streaming and machine learning models. The system will process real-time data streams, identify unusual patterns, and generate alerts to improve traffic management and air quality.

## Objectives

*   Implement a real-time data processing pipeline with Spark Structured Streaming.
*   Integrate traffic and pollution data sources from Madrid.
*   Develop and train machine learning models for anomaly detection.
*   Generate real-time alerts for unusual behavior.
*   Visualize the results in an interactive dashboard with Streamlit.

## Tools and Technologies

*   **Language:** Python
*   **Data Processing:** Apache Spark Structured Streaming (PySpark)
*   **Machine Learning:** MLlib, Scikit-learn
*   **Visualization:** Streamlit
*   **Dependency Management:** Poetry
*   **Containerization:** Docker
*   **Version Control:** Git, GitHub
*   **Experiment Tracking:** MLflow

## Repository Structure

## Installation

1.  Clone the repository:

    ```bash
    git clone git@github.com:JaviOsuna/anomaly_detection_streaming.git
    ```
2.  Navigate to the project directory:

    ```bash
    cd anomaly_detection_streaming
    ```
3. To intall Poetry
    ```powershell
    curl -sSL https://install.python-poetry.org | python -
    ```


4. Create a virtual environment with Poetry (we can see details in the `pyproject.toml` file):

    ```powershell
    poetry init
    ```

    ```powershell
    poetry add pyspark
    poetry add polars
    poetry add mlflow
    poetry add streamlit
    ```

5. To use the environment you have created with *poetry* we shoud run every time
    ```powershell
    poetry run python "file-root"
    ```

## Usage

(Instructions for running the pipeline, training the models, launching the dashboard, etc. will be added here.)

## Project Status

(You can indicate the project's progress, completed tasks, pending tasks, etc. here.)

## Contact

(Your name and email address)

## License

(Optional: you can specify the project's license)