FROM python:3.11-slim

WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY main.py .

# Открываем порт
EXPOSE 8082

# Запускаем приложение
CMD ["python", "main.py"]

