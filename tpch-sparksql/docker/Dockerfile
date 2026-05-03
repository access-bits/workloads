FROM ubuntu:22.04

WORKDIR /

# ── System dependencies ───────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    gcc \
    make \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── sbt ───────────────────────────────────────────────────────────────────
RUN curl -fL "https://github.com/sbt/sbt/releases/download/v1.9.9/sbt-1.9.9.tgz" \
    | tar xz -C /usr/local && \
    ln -sf /usr/local/sbt/bin/sbt /usr/local/bin/sbt

# ── Copy tpch-spark source ────────────────────────────────────────────────
COPY tpch-spark /tpch-spark

# ── Build dbgen ───────────────────────────────────────────────────────────
WORKDIR /tpch-spark/dbgen
RUN cp makefile.suite makefile && \
    sed -i 's/^CC\s*=.*/CC = gcc/' makefile && \
    sed -i 's/^DATABASE\s*=.*/DATABASE = DB2/' makefile && \
    sed -i 's/^MACHINE\s*=.*/MACHINE = LINUX/' makefile && \
    sed -i 's/^WORKLOAD\s*=.*/WORKLOAD = TPCH/' makefile && \
    make

# ── Build tpch-spark jar ─────────────────────────────────────────────────
WORKDIR /tpch-spark
RUN sbt -batch package

# ── Download Spark 3.3.0 standalone (for spark-submit) ───────────────────
RUN wget -q https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && \
    tar xzf spark-3.3.0-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.3.0-bin-hadoop3 /opt/spark && \
    rm spark-3.3.0-bin-hadoop3.tgz

ENV PATH="/opt/spark/bin:$PATH"
ENV SPARK_HOME="/opt/spark"

# ── Copy scripts ──────────────────────────────────────────────────────────
COPY scripts/generate.sh /scripts/generate.sh
COPY scripts/run.sh      /scripts/run.sh
RUN chmod +x /scripts/generate.sh /scripts/run.sh

WORKDIR /
