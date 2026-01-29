FROM apache/spark:3.5.1

USER root

# Install Python dependencies and basic tools
RUN apt-get update && \
    apt-get install -y python3-pip python3-dev build-essential vim nano bash-completion && \
    # Adding python-dotenv to support your .env files
    pip3 install jupyterlab pyspark numpy pandas ipython python-dotenv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create directory structure inside container
RUN mkdir -p /home/spark/.local/share/jupyter/runtime && \
    mkdir -p /opt/spark/notebooks /opt/spark/data /opt/spark/src /opt/spark/conf && \
    chown -R spark:spark /home/spark /opt/spark

USER spark
ENV SHELL /bin/bash

# Critical: Adding /opt/spark to PYTHONPATH so imports work across src/ and jobs/
ENV PYTHONPATH=$PYTHONPATH:/opt/spark

WORKDIR /opt/spark
EXPOSE 8888 4040

CMD ["python3", "-m", "jupyterlab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''"]