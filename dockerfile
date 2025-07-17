FROM python:3.10-slim

# Instala Java 17 (compatible con Spark 3.3+)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Establece las variables de entorno para Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Instala Poetry
RUN pip install poetry

# Copia los archivos de configuración de Poetry primero
COPY pyproject.toml poetry.lock* /app/

WORKDIR /app

# Instala las dependencias del proyecto
RUN poetry install --no-root

# Copia el resto del código del proyecto
COPY . /app

EXPOSE 5000
EXPOSE 8501

CMD ["bash"]