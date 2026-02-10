import requests  # Para fazer requisições HTTP 
import json      # Para trabalhar com arquivos JSON
import logging   # Para criar mensagens de log 
import os        # Para trabalhar com pastas e arquivos
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Configurar logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_users():
    """
    Extrai dados de usuários da API DummyJSON
    e salva em arquivo JSON local
    """
    api_url = os.getenv("API_URL")
    output_path = os.getenv("RAW_DATA_PATH")
    
    # Valida se as variáveis foram configuradas
    if not api_url:
        raise ValueError("Variável API_URL não configurada! Configure no arquivo .env")
    if not output_path:
        raise ValueError("Variável RAW_DATA_PATH não configurada! Configure no arquivo .env")
    
    logger.info(f" Iniciando extração de: {api_url}")
    logger.info(f" Destino: {output_path}")
    
    # Pega só o caminho da pasta (tira o nome do arquivo)
    output_dir = os.path.dirname(output_path)
    
    # Cria a pasta se não existir (exist_ok=True não dá erro se já existir)
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f" Pasta criada/verificada: {output_dir}")
    
    
    try:
        # requests.get() busca os dados da URL
        response = requests.get(api_url)
        logger.info(f"Resposta recebida! Status: {response.status_code}")
        
        # 200 = sucesso, 404 = não encontrado, 500 = erro no servidor
        if response.status_code != 200:
            logger.error(f" Erro! API retornou status: {response.status_code}")
            raise Exception(f"Falha na API: {response.status_code}")
        
        # .json() transforma a resposta em dicionário Python
        data = response.json()
        
        # Conta quantos usuários vieram
        total_users = len(data.get('users', []))
        logger.info(f" Usuários extraídos: {total_users}")
        

        # 'w' = write (escrever), encoding='utf-8' = aceita acentos
        with open(output_path, 'w', encoding='utf-8') as file:
            # Salva o dicionário como JSON no arquivo
            # indent=4 deixa formatado bonitinho
            json.dump(data, file, indent=4, ensure_ascii=False)
        
        logger.info(f" Arquivo salvo em: {output_path}")
        
        logger.info(" Extração concluída com sucesso!")
        return output_path
        
    except requests.exceptions.RequestException as e:
        # Se der erro de internet/conexão
        logger.error(f" Erro de conexão: {e}")
        raise
    except Exception as e:
        # Qualquer outro erro
        logger.error(f" Erro inesperado: {e}")
        raise

# Se rodar este arquivo direto (python extract.py), testa a função
if __name__ == "__main__":
    extract_users()