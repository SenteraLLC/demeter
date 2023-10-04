FROM python:3.10 as base

# ARG SSH_PRIVATE_KEY
# RUN mkdir /root/.ssh/
# RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa
# RUN chmod 600 /root/.ssh/id_rsa
# RUN touch /root/.ssh/known_hosts
# RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

# RUN mkdir -p -m 0700 root/.ssh
# RUN ssh-keyscan -t rsa github.com >> root/.ssh/known_hosts
# COPY ~/.ssh/config /root/.ssh
# RUN chmod 0700 /root/.ssh
# RUN ssh -T git@github.com

# FROM base as builder

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.6.1
# PYTHONPATH=${PYTHONPATH}:${PWD}

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


# RUN mkdir -p ~/.ssh && \
#     ssh-keyscan -t rsa -H github.com >> -f ~/.ssh/known_hosts
RUN pip3 install "poetry==$POETRY_VERSION"
RUN poetry config virtualenvs.in-project true

# Copy only requirements to cache them in docker layer
# COPY pyproject.toml poetry.lock README.md ./
# or this if your file is stored in $PROJECT_NAME, assuming `myproject`
# COPY myproject ./myproject

# Set up SSH:
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

WORKDIR /app
COPY . .

RUN poetry install --only=main --no-root --no-interaction --no-ansi

# COPY demeter ./demeter
# RUN mkdir -p root/.ssh && \
#     touch root/.ssh/known_hosts && \
#     chmod 0700 root/.ssh && \
#     ssh-keyscan -t rsa github.com >> root/.ssh/known_hosts
# RUN ssh-add -L
# RUN ssh -T git@github.com
# RUN --mount=type=ssh poetry install --only=main --no-root --no-interaction --no-ansi
# RUN poetry build

# CMD ["poetry", "run", "python3", "-c", "print('Docker/Python is set up.')"]
# RUN poetry run python3 -c "print('Docker/Python is set up.')"