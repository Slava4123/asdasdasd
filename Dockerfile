FROM python:3.12-slim

WORKDIR /app

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Установка Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Добавление Poetry в PATH
ENV PATH="/root/.local/bin:${PATH}"

# Копируем зависимости
COPY pyproject.toml poetry.lock ./

# Установка зависимостей проекта
RUN poetry install --no-interaction --no-root

# Копируем исходный код
COPY . .

# Команда для запуска
CMD ["poetry", "run", "python", "-m", "server.server"]