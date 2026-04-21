FROM akrishnaams/ubuntu2204:oh-my-bash AS base
# FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Set default directories (can be overridden)
ENV GRAPH_DIR=/root/gapbs/benchmark/graphs
ENV OUTPUT_DIR=/root/gapbs/benchmark/out

# Install all dependencies in one layer and clean up in same command
RUN apt-get update -qq && \
    apt-get install -y -qq \
        make \
        g++ \
        wget \
        tar \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories (using ENV variables)
RUN mkdir -p ${OUTPUT_DIR} && \
    mkdir -p ${GRAPH_DIR}/raw

WORKDIR /root/gapbs

# Copy source files
COPY ./src /root/gapbs/src
COPY ./pagerank.mk /root/gapbs/Makefile

# Build binaries
RUN make all

# Set default command
CMD ["/bin/bash"]

