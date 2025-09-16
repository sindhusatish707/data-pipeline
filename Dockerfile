FROM apache/airflow:3.0.6

# Switch to airflow user before installing Python packages
USER airflow

# Copy requirements file
COPY requirements.txt .

# Install packages as airflow user
RUN pip install --no-cache-dir -r requirements.txt
