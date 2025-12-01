FROM apache/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# FIX: Create the Ivy cache directory and give permission to the non-root user (185)
RUN mkdir -p /home/spark/.ivy2/cache && \
    mkdir -p /home/spark/.ivy2/jars && \
    chown -R 185:185 /home/spark

# Switch back to the default Spark user
USER 185

WORKDIR /opt/spark/work-dir
COPY . .