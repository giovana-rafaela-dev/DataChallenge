# começa com a imagem oficial do airflow
FROM apache/airflow:3.1.0  

USER root
# Atualiza e instala lista de pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow 

# Pega o arquivo e coloca dentroda imagem Docker e Instala as dependências Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt