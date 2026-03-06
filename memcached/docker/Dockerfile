FROM ubuntu:22.04

WORKDIR /

# Update package lists and install dependencies
RUN apt-get update && apt-get install -y \
    maven \
    git \
    vim \
    openjdk-17-jre-headless \
    python3 \
    python-is-python3 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Clone YCSB repository
RUN git clone https://github.com/brianfrankcooper/YCSB.git

# Build YCSB with memcached binding
WORKDIR /YCSB
RUN mvn -pl site.ycsb:memcached-binding -am clean package -DskipTests

# Set working directory to YCSB
WORKDIR /YCSB

# Copy benchmark scripts and config
COPY src/yscb-config.json /YCSB/config.json
COPY src/load.sh /YCSB/load.sh
COPY src/run.sh /YCSB/run.sh
RUN chmod +x /YCSB/load.sh /YCSB/run.sh
