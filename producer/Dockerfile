# ==============================================================================
# Dockerfile para o Produtor de Telemetria IoT (SmartFactory)
# Baseado em Python 3.11 Slim para eficiência e compatibilidade
# ==============================================================================

FROM python:3.11-slim

# Evita geração de arquivos .pyc e força flush imediato nos logs (stdout/stderr)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instala dependências do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte do produtor
COPY sensor_producer.py .

# Executa o produtor
CMD ["python", "sensor_producer.py"]
