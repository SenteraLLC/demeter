FROM python:3.10 as base

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.6.1

# Install PostGIS (needed to use raster2pgsql)
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --no-install-recommends \
    python3-setuptools \
    python3-pip \
    python3-venv \
    python3-dev \
    ssh \
    git \
    postgis \
    ffmpeg \
    libsm6 \
    libxext6 \
    libpq-dev \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip3 install "poetry==$POETRY_VERSION"
RUN poetry config virtualenvs.in-project true

# Set up SSH:
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

WORKDIR /app
COPY . .

RUN poetry install
