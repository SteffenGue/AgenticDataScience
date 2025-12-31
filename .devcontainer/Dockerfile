FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    curl \
    sudo \
    git \
    postgresql-client \
    libpq-dev

RUN groupadd vscode \
    && useradd -g vscode -m vscode \
    && echo vscode ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode

USER vscode

WORKDIR /workspace
