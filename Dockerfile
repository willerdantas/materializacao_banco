# Imagem simples para rodar o app Flask (servidor de desenvolvimento do
# próprio Flask — mesmo comportamento de hoje, incluindo debug/reloader).
FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary já traz o libpq embutido, então não precisa de
# build-essential/libpq-dev aqui.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Garante que o diretório de dados (configs, usuários, chave de encriptação)
# exista mesmo se não vier montado por volume.
RUN mkdir -p /app/data

EXPOSE 5310

CMD ["python", "importacao.py"]
