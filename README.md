# 🚀 DataChallenge - Pipeline ETL com Airflow & PySpark

Pipeline de engenharia de dados automatizado que extrai dados de usuários da API DummyJSON, transforma com PySpark e disponibiliza em formato CSV estruturado.

---

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Stack Tecnológica](#stack-tecnológica)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Execução](#instalação-e-execução)
- [Pipeline ETL](#pipeline-etl)
- [Configuração](#configuração)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) completo utilizando as melhores práticas de engenharia de dados:

- **Extract:** Consumo de API REST (DummyJSON) com tratamento de erros
- **Transform:** Processamento distribuído com PySpark (explode, flatten, normalização)
- **Load:** Salvamento em CSV estruturado
- **Orquestração:** Apache Airflow com CeleryExecutor
- **Infraestrutura:** Docker Compose com 6 containers

**Objetivo:** Demonstrar capacidade end-to-end em engenharia de dados, desde consumo de APIs até orquestração containerizada.

---

## 🏗️ Arquitetura

### Diagrama de Containers

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   Scheduler     │────▶│  Webserver   │────▶│   Worker    │
└─────────────────┘     └──────────────┘     └─────────────┘
         │                      │                     │
         └──────────────────────┴─────────────────────┘
                                │
                     ┌──────────┴──────────┐
                     │                     │
              ┌──────▼──────┐      ┌──────▼──────┐
              │  PostgreSQL │      │    Redis    │
              └─────────────┘      └─────────────┘
```

### Fluxo de Dados

```
API (DummyJSON)
    ↓ [extract.py]
JSON Bruto (data/raw/raw_data.json)
    ↓ [transform.py - PySpark]
CSV Estruturado (data/processed/users.csv)
```

---

## 💻 Stack Tecnológica

| Tecnologia              | Versão | Função                    |
| ----------------------- | ------ | ------------------------- |
| Apache Airflow          | 3.1.0  | Orquestração de pipelines |
| PySpark                 | 3.5.3  | Processamento distribuído |
| Python                  | 3.x    | Linguagem principal       |
| Docker & Docker Compose | -      | Containerização           |
| PostgreSQL              | -      | Metadatabase do Airflow   |
| Redis                   | -      | Message broker (Celery)   |
| Java                    | 17     | Runtime para PySpark      |

**Bibliotecas Python:**

- `requests` - Requisições HTTP
- `pandas` - Manipulação de dados
- `python-dotenv` - Gestão de variáveis de ambiente

---

## 📂 Estrutura do Projeto

```
DataChallenge/
├── dags/
│   └── data_pipeline.py          # DAG do Airflow (orquestração)
├── src/
│   ├── scripts_extract/
│   │   └── extract.py            # Extração da API
│   └── scripts_transform/
│       └── transform.py          # Transformação com PySpark
├── data/
│   ├── raw/                      # Dados brutos (JSON)
│   └── processed/                # Dados processados (CSV)
├── config/
│   └── airflow.cfg               # Configurações do Airflow
├── logs/                         # Logs de execução
├── plugins/                      # Plugins customizados
├── docker-compose.yaml           # Orquestração de containers
├── Dockerfile                    # Imagem customizada
├── requirements.txt              # Dependências Python
└── README.md                     # Este arquivo
```

---

## 🔧 Pré-requisitos

### No seu computador:

- **Docker Desktop** instalado e rodando
  - Windows: [Download Docker Desktop](https://docs.docker.com/desktop/install/windows-install/)
  - Linux: Docker Engine + Docker Compose
  - Mac: [Download Docker Desktop](https://docs.docker.com/desktop/install/mac-install/)

- **Git** (para clonar o repositório)

### Verificar instalação:

```bash
docker --version
docker-compose --version
git --version
```

---

## 🚀 Instalação e Execução

### 1️⃣ Clonar o Repositório

```bash
git clone <URL_DO_SEU_REPOSITORIO>
cd DataChallenge
```

### 2️⃣ Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto (se não existir):

```env
# API Configuration
API_URL=https://dummyjson.com/users

# Data Paths (dentro do container)
RAW_DATA_PATH=/opt/airflow/data/raw/raw_data.json
PROCESSED_DATA_PATH=/opt/airflow/data/processed/users.csv

# Airflow Settings
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_PIP_ADDITIONAL_REQUIREMENTS=
```

### 3️⃣ Build da Imagem Docker

```bash
docker-compose build
```

Este comando:

- Baixa a imagem base do Airflow 3.1.0
- Instala Java 17 (necessário para PySpark)
- Instala dependências Python (PySpark, requests, pandas)

⏱️ **Tempo estimado:** 5-10 minutos (na primeira vez)

### 4️⃣ Inicializar o Airflow

```bash
docker-compose up airflow-init
```

Este comando:

- Cria o banco de dados PostgreSQL
- Configura usuário admin padrão
- Prepara o ambiente do Airflow

### 5️⃣ Subir os Containers

```bash
docker-compose up -d
```

Este comando sobe 6 containers:

- `postgres` - Banco de dados
- `redis` - Message broker
- `airflow-webserver` - Interface web (porta 8080)
- `airflow-scheduler` - Agendador de DAGs
- `airflow-worker` - Executor de tasks
- `airflow-triggerer` - Gerenciador de triggers

⏱️ **Aguarde 30-60 segundos** para todos os serviços iniciarem

### 6️⃣ Acessar a Interface do Airflow

Abra no navegador:

```
http://localhost:8080
```

**Credenciais padrão:**

- Username: `airflow`
- Password: `airflow`

### 7️⃣ Executar o Pipeline

1. Na interface do Airflow, localize a DAG `data_pipeline_users`
2. Ative a DAG (toggle no canto esquerdo)
3. Clique no botão ▶️ (Play) para executar manualmente
4. Acompanhe o progresso na aba "Graph" ou "Grid"

### 8️⃣ Verificar Resultados

Após execução bem-sucedida:

```bash
# Ver o JSON bruto extraído
cat data/raw/raw_data.json

# Ver o CSV processado
cat data/processed/users.csv
```

---

## ⚙️ Pipeline ETL

### 🔹 Extract (`extract.py`)

**Responsabilidade:** Consumir API DummyJSON e salvar JSON local

**Funcionamento:**

1. Carrega variáveis de ambiente (API_URL, RAW_DATA_PATH)
2. Valida configurações
3. Cria diretório de saída se não existir
4. Faz requisição GET para API
5. Valida status code (200 = sucesso)
6. Salva JSON com formatação (indent=4, UTF-8)

**Tratamento de erros:**

- `ValueError` - Se variáveis de ambiente não configuradas
- `RequestException` - Se falha de conexão ou timeout
- `Exception` - Se status code diferente de 200

**Output:** `data/raw/raw_data.json`

---

### 🔹 Transform (`transform.py`)

**Responsabilidade:** Processar JSON aninhado com PySpark e gerar CSV

**Funcionamento:**

1. **Inicia SparkSession:**

   ```python
   spark = SparkSession.builder \
       .appName("TransformUsers") \
       .master("local[*]") \
       .getOrCreate()
   ```

   - `local[*]` = usa todos os cores da CPU

2. **Lê JSON com estrutura aninhada:**

   ```python
   df = spark.read.option("multiLine", "true").json(raw_data_path)
   ```

3. **Explode do array `users`:**

   ```python
   users_df = df.select(explode(col("users")).alias("user"))
   ```

   - Transforma array em múltiplas linhas
   - Cada usuário vira uma linha

4. **Flatten de estruturas hierárquicas:**

   ```python
   col("user.address.city").alias("address_city")
   col("user.hair.color").alias("hair_color")
   ```

   - Acessa campos aninhados com notação de ponto
   - Traz para o nível superior

5. **Seleciona 23 campos relevantes:**
   - Informações pessoais (nome, idade, email, etc.)
   - Características físicas (altura, peso, cor dos olhos)
   - Endereço completo (rua, cidade, estado, CEP)
   - Dados adicionais (tipo sanguíneo, IP)

6. **Salva CSV otimizado:**

   ```python
   final_df.coalesce(1).write.mode("overwrite").csv(temp_output, header=True)
   ```

   - `coalesce(1)` = gera apenas 1 arquivo (não particionado)
   - `mode("overwrite")` = sobrescreve se já existir
   - `header=True` = inclui cabeçalho com nomes das colunas

7. **Renomeia arquivo:**
   - Spark gera nome automático `part-xxxxx.csv`
   - Usa `glob` para encontrar
   - Renomeia para `users.csv`
   - Remove pasta temporária

**Output:** `data/processed/users.csv`

---

### 🔹 Orquestração (`data_pipeline.py`)

**Responsabilidade:** Conectar tasks e gerenciar dependências

**Estrutura da DAG:**

```python
with DAG(
    dag_id='data_pipeline_users',
    schedule=None,  # Execução manual
    catchup=False,  # Não executa datas passadas
    tags=['etl', 'dummyjson', 'pyspark']
) as dag:
    extract_task >> transform_task
```

**Características:**

- **Operator:** PythonOperator (executa funções Python)
- **Dependência:** Extract → Transform (operador `>>`)
- **Retry:** 1 tentativa em caso de falha
- **Importação dinâmica:** Usa `importlib` para scripts em pastas com nomes especiais

---

## 🔐 Configuração

### Variáveis de Ambiente

Definidas no `docker-compose.yaml`:

| Variável                       | Valor                                 | Descrição                    |
| ------------------------------ | ------------------------------------- | ---------------------------- |
| `API_URL`                      | https://dummyjson.com/users           | Endpoint da API              |
| `RAW_DATA_PATH`                | /opt/airflow/data/raw/raw_data.json   | Destino do JSON              |
| `PROCESSED_DATA_PATH`          | /opt/airflow/data/processed/users.csv | Destino do CSV               |
| `AIRFLOW__CORE__EXECUTOR`      | CeleryExecutor                        | Executor distribuído         |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | false                                 | Não carregar DAGs de exemplo |

### Volumes Docker

Mapeamento host → container:

```yaml
volumes:
  - ./dags:/opt/airflow/dags # DAGs
  - ./logs:/opt/airflow/logs # Logs
  - ./src:/opt/airflow/src # Scripts Python
  - ./data:/opt/airflow/data # Dados (raw/processed)
  - ./config:/opt/airflow/config # Configurações
  - ./plugins:/opt/airflow/plugins # Plugins customizados
```

---

## 🐛 Troubleshooting

### Problema: DAG não aparece na interface

**Solução:**

```bash
# Verificar logs do scheduler
docker-compose logs airflow-scheduler | tail -50

# Restartar scheduler
docker-compose restart airflow-scheduler

# Aguardar 30 segundos e atualizar página
```

### Problema: Erro "404 Not Found - DAG not found"

**Causas comuns:**

- Erro de sintaxe no código Python
- Caminho de importação incorreto
- Pasta src não sincronizada

**Solução:**

```bash
# Testar importação da DAG
docker-compose exec airflow-scheduler python -c "from dags.data_pipeline import *; print('OK')"

# Se der erro, verificar sintaxe
docker-compose exec airflow-scheduler python -m py_compile /opt/airflow/dags/data_pipeline.py
```

### Problema: Task falha com erro de PySpark

**Solução:**

```bash
# Verificar se Java está instalado no container
docker-compose exec airflow-worker java -version

# Verificar logs da task
# (na interface do Airflow, clique na task → Logs)
```

### Problema: Containers não sobem

**Solução:**

```bash
# Verificar status
docker-compose ps

# Ver logs de erros
docker-compose logs

# Rebuild forçado
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```

### Problema: Porta 8080 já em uso

**Solução:**

```bash
# Matar processo que usa a porta
# Windows:
netstat -ano | findstr :8080
taskkill /PID <PID> /F

# Linux/Mac:
lsof -ti:8080 | xargs kill -9

# Ou alterar porta no docker-compose.yaml:
# ports: - "8081:8080"  # Usa porta 8081 no host
```

---

## 📊 Dados Processados

### Campos Extraídos (23 colunas)

| Campo              | Tipo   | Descrição           |
| ------------------ | ------ | ------------------- |
| id                 | int    | ID único do usuário |
| firstName          | string | Primeiro nome       |
| lastName           | string | Sobrenome           |
| maidenName         | string | Nome de solteira    |
| age                | int    | Idade               |
| gender             | string | Gênero              |
| email              | string | Email               |
| phone              | string | Telefone            |
| username           | string | Username            |
| birthDate          | string | Data de nascimento  |
| image              | string | URL da foto         |
| bloodGroup         | string | Tipo sanguíneo      |
| height             | float  | Altura (cm)         |
| weight             | float  | Peso (kg)           |
| eyeColor           | string | Cor dos olhos       |
| hair_color         | string | Cor do cabelo       |
| hair_type          | string | Tipo do cabelo      |
| ip                 | string | Endereço IP         |
| address_address    | string | Endereço (rua)      |
| address_city       | string | Cidade              |
| address_state      | string | Estado              |
| address_stateCode  | string | Código do estado    |
| address_postalCode | string | CEP                 |

---

## 🛠️ Comandos Úteis

```bash
# Ver status dos containers
docker-compose ps

# Ver logs de um serviço específico
docker-compose logs airflow-scheduler -f

# Parar todos os containers
docker-compose down

# Parar e remover volumes (CUIDADO: apaga dados do banco)
docker-compose down -v

# Rebuild após mudanças no Dockerfile
docker-compose build

# Acessar shell do container
docker-compose exec airflow-worker bash

# Ver recursos usados
docker stats
```

---

## 👤 Autor

**Giovana Rafaela**

📧 Contato: [giovana.rafaela7@hotmail.com]  
🔗 LinkedIn: [www.linkedin.com/in/giovana-rafaela-da-silva]  
🐙 GitHub: [https://github.com/giovana-rafaela-dev]

---

**Desenvolvido com ❤️ para demonstrar habilidades em Engenharia de Dados**
