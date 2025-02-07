FROM python:3.12-slim

WORKDIR /app

RUN pip install poetry

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-root

COPY . .

CMD ["poetry", "run", "python", "-m", "server.server"]
