# ==========================================
# UNIVERSAL PYSPARK DOCKER SETUP
# ==========================================
# This setup works on Mac, Windows, and Linux
# No need to install Java, Scala, or Spark locally!

# Dockerfile
FROM jupyter/pyspark-notebook:latest

USER root

# Install additional packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID

# Install additional Python packages
RUN pip install --no-cache-dir \
    pandas \
    matplotlib \
    seaborn \
    plotly \
    jupyterlab

# Set working directory
WORKDIR /home/jovyan/work

# Copy workshop files
COPY workshop.py ./
COPY *.csv ./

# Expose Jupyter port and Spark UI port
EXPOSE 8888 4040

# Start Jupyter Lab
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]