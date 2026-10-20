FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml ./
COPY collectors ./collectors
COPY database ./database
COPY streaming ./streaming
COPY config ./config
RUN pip install '.[collectors]' && useradd --create-home --uid 10001 crawler
USER crawler
CMD ["python", "-m", "collectors", "run", "--source", "all", "--publish"]
