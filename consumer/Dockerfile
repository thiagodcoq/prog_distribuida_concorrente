# ==============================================================================
# Dockerfile para o Consumidor e Processador de Dados IoT (SmartFactory)
# Baseado em Python 3.11 Slim para eficiência e compatibilidade
# ==============================================================================

FROM python:3.11-slim

# Evita arquivos .pyc e garante saída imediata em stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Cria o diretório compartilhado de logs de alertas
RUN mkdir -p /var/log/smartfactory

# Instala dependências do Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código-fonte do consumidor
COPY data_processor.py .

# Executa o consumidor
CMD ["python", "data_processor.py"]
