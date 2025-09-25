FROM apache/airflow:3.0.6

# Install system dependencies as root first (Java required for Spark)
USER root

# Install OpenJDK 17 and other tools
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV SPARK_LOCAL_IP=127.0.0.1

# Verify Java installation and show actual paths
RUN echo "Java installation verification:" && \
    ls -la /usr/lib/jvm/ && \
    echo "JAVA_HOME: $JAVA_HOME" && \
    echo "Java binary location: $(which java)" && \
    ls -la $JAVA_HOME/bin/java && \
    java -version

# Switch to airflow user to keep container secure
USER airflow

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Final verification as airflow user
RUN echo "Final verification as airflow user:" && \
    echo "JAVA_HOME: $JAVA_HOME" && \
    echo "PATH: $PATH" && \
    java -version && \
    python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
