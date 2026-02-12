import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

def extract_users():
    """Extrai dados de usuários da API DummyJSON e salva em JSON local"""
    api_url = os.getenv("API_URL")
    output_path = os.getenv("RAW_DATA_PATH")
    
    if not api_url or not output_path:
        raise ValueError(" Variáveis API_URL e RAW_DATA_PATH devem estar configuradas!")
    # Criar pasta se não existir
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        response = requests.get(api_url)
        
        if response.status_code != 200:
            raise Exception(f"API retornou erro: {response.status_code}")
        
        data = response.json()
        
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        
        return output_path
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro de conexão: {e}")

if __name__ == "__main__":
    extract_users()