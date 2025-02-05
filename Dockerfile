FROM python:3.12-slim

WORKDIR /app

# Устанавливаем Poetry
RUN pip install --no-cache-dir poetry

# Копируем файлы Poetry
COPY pyproject.toml poetry.lock ./

# Устанавливаем зависимости с помощью Poetry
RUN poetry install --no-root --no-interaction --no-ansi

# Копируем остальные файлы
COPY . .

# Команда для запуска сервера
CMD ["poetry", "run", "python", "server/server.py"]
