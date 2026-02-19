import requests  # requisições HTTP
import json  # dados JSON
import os  # sistema operacional (arquivos, pastas)
from dotenv import load_dotenv  # variáveis de ambiente de arquivo .env

load_dotenv()  # Executa a função que lê o arquivo .env e carrega as variáveis

def extract_users():  # Define a função principal de extração
    """Extrai dados de usuários da API DummyJSON e salva em JSON local"""
    api_url = os.getenv("API_URL")  # Busca a URL da API nas variáveis de ambiente
    output_path = os.getenv("RAW_DATA_PATH")  # Busca o caminho onde salvar o arquivo JSON
    
    if not api_url or not output_path:  # Verifica se as variáveis foram definidas
        raise ValueError(" Variáveis API_URL e RAW_DATA_PATH devem estar configuradas!")  # Lança erro se alguma variável estiver vazia
    
    output_dir = os.path.dirname(output_path)  # Extrai o diretório do caminho completo do arquivo
    os.makedirs(output_dir, exist_ok=True)  # Cria o diretório se não existir (exist_ok=True evita erro se já existir)
    
    try:  # Inicia bloco de tratamento de exceções
        response = requests.get(api_url)  # Faz requisição GET para a API
        
        if response.status_code != 200:  # Verifica se a resposta foi bem-sucedida (200 = OK)
            raise Exception(f"API retornou erro: {response.status_code}")  # Lança exceção se status code for diferente de 200
        
        data = response.json()  # Converte a resposta JSON para dicionário Python
        
        with open(output_path, 'w', encoding='utf-8') as file:  # Abre arquivo para escrita com encoding UTF-8
            json.dump(data, file, indent=4, ensure_ascii=False)  # Salva dados no arquivo (indent=4 formata, ensure_ascii=False permite acentos)
        
        return output_path  # Retorna o caminho do arquivo salvo
        
    except requests.exceptions.RequestException as e:  # Captura erros de requisição (timeout, conexão, DNS)
        raise Exception(f"Erro de conexão: {e}")  # Lança exceção personalizada com detalhes do erro

if __name__ == "__main__":  # Verifica se o script está sendo executado diretamente (não importado)
    extract_users()  # Executa a função de extração