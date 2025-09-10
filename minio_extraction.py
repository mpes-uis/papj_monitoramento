import logging
import os
import platform
import re
import shutil
import time
from datetime import datetime
from pathlib import Path # Usar pathlib para manipulação de caminhos
from typing import List, Dict, Any, Optional
import tempfile # Para arquivos temporários

import fitz  # PyMuPDF
import pytesseract
from PIL import Image
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError
from elasticsearch.helpers import bulk
import pyodbc


# Carrega as variáveis de ambiente do .env (se existir)
load_dotenv()

# Configuração básica de logging
# Adicionar worker_id ao formato de log pode ser útil
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(processName)s - %(filename)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Classes de Configuração (melhor que variáveis globais soltas) ---
class AppConfig:
    SQL_SERVER_CNXN_STR: Optional[str] = os.getenv('SQL_SERVER_CNXN_STR')
    ELASTICSEARCH_HOSTS: List[str] = (os.getenv('ELASTICSEARCH_HOSTS') or 'http://localhost:9200').split(',')
    ELASTICSEARCH_USER: Optional[str] = os.getenv('ELASTICSEARCH_USER')
    ELASTICSEARCH_PWD: Optional[str] = os.getenv('ELASTICSEARCH_PWD')
    
    ES_INDEX_TEXT: str = os.getenv('ES_INDEX_TEXT', 'gampes_textual')
    ES_INDEX_PAGE: str = os.getenv('ES_INDEX_PAGE', 'gampes_textual_paginas')

    MINIO_ENDPOINT: str = os.getenv('MINIO_ENDPOINT')
    MINIO_ACCESS_KEY: str = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = os.getenv('MINIO_SECRET_KEY')

    TESSERACT_TEST_IMAGE: str = os.getenv('TESSERACT_TEST_IMAGE', 'test.png') # Opcional

    # Diretório base para saída temporária, se não usar tempfile para tudo
    # OUTPUT_BASE_DIR: Path = Path(os.getenv('OUTPUT_BASE_DIR', './ocr_output'))

    @staticmethod
    def _validate_config():
        if not AppConfig.SQL_SERVER_CNXN_STR:
            logger.error("Variável de ambiente SQL_SERVER_CNXN_STR não definida.")
            raise ValueError("SQL_SERVER_CNXN_STR não configurada.")
        logger.info('String de conexão SQL Server carregada.')
        logger.info('Hosts Elasticsearch: %s', AppConfig.ELASTICSEARCH_HOSTS)
        logger.info('Configuração MinIO: %s', {"endpoint": AppConfig.MINIO_ENDPOINT})


AppConfig._validate_config()
# AppConfig.OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)



def get_db_connection(conn_str: str) -> pyodbc.Connection:
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logger.error(f"Erro de conexão com SQL Server (SQLSTATE: {sqlstate}): {ex}")
        raise # Re-levanta para ser tratado pelo chamador


def get_minio_file_path(id_documento_mni: str, db_conn) -> str:
    # Primeira query - busca pelo arquivo renderizado
    query_renderizado = """
        SELECT a.path as caminho_minio
        FROM MPES.dbo.documentos d WITH (NOLOCK)
        LEFT JOIN MPES.dbo.arquivos a WITH (NOLOCK) ON a.id = d.id_arquivo_renderizado
        WHERE d.Id = ?
    """
    
    # Segunda query - busca pelo arquivo original (se a primeira não retornar nada)
    query_original = """
        SELECT a.path as caminho_minio
        FROM MPES.dbo.documentos d WITH (NOLOCK)
        LEFT JOIN MPES.dbo.arquivos a WITH (NOLOCK) ON a.id = d.id_arquivo_externo
        WHERE d.Id = ?
    """
    
    with db_conn.cursor() as cursor:
        # Tenta buscar o caminho do arquivo renderizado primeiro
        cursor.execute(query_renderizado, (id_documento_mni,))
        row = cursor.fetchone()
        
        # Se não encontrou ou o path é NULL, tenta buscar o caminho do arquivo original
        if not row or not row[0]:
            cursor.execute(query_original, (id_documento_mni,))
            row = cursor.fetchone()
        
        # Retorna o path se encontrou, ou None se não encontrou em nenhuma das queries
        return row[0] if row and row[0] else None
    

# if __name__ == "__main__":
#     try:
#         conn = get_db_connection(AppConfig.SQL_SERVER_CNXN_STR)
#         # Teste simples de conexão
#         with conn.cursor() as test_cursor:
#             test_cursor.execute("SELECT 1 as test")
#             test_result = test_cursor.fetchone()
#             print(f"Teste de conexão: {test_result[0] if test_result else 'falha'}")
            
#         doc_id = "9332334"
#         print(f"Testando get_minio_file_path para o documento {doc_id}...")
#         caminho = get_minio_file_path(doc_id, conn)
#         print(f"Caminho MinIO para o documento {doc_id}: {caminho}")
#     except Exception as e:
#         logger.error(f"Erro ao testar get_minio_file_path: {e}")
#         raise  # Re-lança a exceção para ver o stack trace completo


import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from urllib.parse import urlparse
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinIODownloader:
    def __init__(self, endpoint_url, access_key, secret_key):
        """
        Inicializa o cliente MinIO
        
        Args:
            endpoint_url (str): URL do servidor MinIO
            access_key (str): Chave de acesso
            secret_key (str): Chave secreta
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        
        # Configurar cliente S3 para MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{endpoint_url}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'  # MinIO geralmente usa esta região
        )
    
    def parse_path(self, path):
        """
        Analisa o path da tabela e extrai informações do bucket e arquivo
        
        Args:
            path (str): Path no formato da tabela
            
        Returns:
            tuple: (bucket_name, object_key)
        """
        # O path parece estar no formato: uuid|tipo.documento|mime_type|extensao
        parts = path.split('|')
        
        if len(parts) < 2:
            raise ValueError(f"Formato de path inválido: {path}")
        
        uuid = parts[0]
        document_type = parts[1]
        
        # Mapear tipos de documento para buckets
        bucket_mapping = {
            'mni.documento.original': 'gampes-mni-documento-original',
            'documento.externo': 'gampes-documento-externo',
            'documento.assinatura': 'gampes-documento-assinatura',
            'documento.renderizado': 'gampes-documento-renderizado',
            'documento.sumarizado': 'gampes-documento-sumarizado',
            'documento.transcricao': 'gampes-documento-transcricao',
            'documento.visualizacao': 'gampes-documento-visualizacao',
            'documento.pessoal': 'gampes-documento-pessoal',
            'autos.movimento': 'gampes-autos-movimento',
            'atividade.nao.procedimental': 'gampes-atividade-nao-procedimental',
            'documento.gerador.denuncia': 'gampes-documento-gerador-denuncia',
            'mni.comprovante': 'gampes-mni-comprovante',
            'mni.documento.renderizado': 'gampes-mni-documento-renderizado'
        }
        
        bucket_name = bucket_mapping.get(document_type)
        if not bucket_name:
            # Fallback: tentar inferir o bucket baseado no tipo
            logger.warning(f"Tipo de documento não mapeado: {document_type}")
            bucket_name = f"gampes-{document_type.replace('.', '-')}"
        
        # O nome do objeto no MinIO inclui a extensão
        if len(parts) >= 4:
            extension = parts[3]
            # Adiciona o ponto se não houver
            if not extension.startswith('.'):
                extension = f".{extension}"
            object_key = f"{uuid}{extension}"
        else:
            object_key = uuid
        
        return bucket_name, object_key
    
    def download_document(self, path, local_directory="downloads", filename=None):
        """
        Baixa um documento do MinIO
        
        Args:
            path (str): Path do documento na tabela
            local_directory (str): Diretório local para salvar
            filename (str): Nome do arquivo (opcional)
            
        Returns:
            str: Caminho completo do arquivo baixado
        """
        try:
            # Analisar o path
            bucket_name, object_key = self.parse_path(path)
            uuid = path.split('|')[0]
            
            # Criar diretório se não existir
            os.makedirs(local_directory, exist_ok=True)
            
            # Definir nome do arquivo
            if not filename:
                filename = object_key
            
            local_path = os.path.join(local_directory, filename)
            
            # Verificar se o objeto existe
            try:
                self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # Tentar encontrar o objeto no bucket
                    found_object = self.find_object_in_bucket(bucket_name, uuid)
                    if found_object:
                        object_key = found_object
                        # Atualizar o nome do arquivo local
                        if not filename or filename == f"{uuid}.pdf":
                            filename = found_object
                            local_path = os.path.join(local_directory, filename)
                        logger.info(f"Objeto encontrado: {bucket_name}/{object_key}")
                    else:
                        logger.error(f"Objeto não encontrado no bucket {bucket_name} para UUID: {uuid}")
                        return None
                else:
                    raise
            
            # Baixar o arquivo
            logger.info(f"Baixando {bucket_name}/{object_key} -> {local_path}")
            self.s3_client.download_file(bucket_name, object_key, local_path)
            
            logger.info(f"Download concluído: {local_path}")
            return local_path
            
        except ClientError as e:
            logger.error(f"Erro ao baixar arquivo: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            return None
    
    def download_multiple_documents(self, documents_data, local_directory="downloads"):
        """
        Baixa múltiplos documentos
        
        Args:
            documents_data (list): Lista de dicionários com dados dos documentos
            local_directory (str): Diretório local para salvar
            
        Returns:
            list: Lista de caminhos dos arquivos baixados com sucesso
        """
        downloaded_files = []
        
        for doc in documents_data:
            try:
                path = doc['path']
                doc_id = doc.get('id', 'unknown')
                
                # Criar nome de arquivo com ID
                parts = path.split('|')
                if len(parts) >= 4:
                    extension = parts[3]
                    filename = f"{doc_id}_{parts[0]}{extension}"
                else:
                    filename = f"{doc_id}_{parts[0]}"
                
                local_path = self.download_document(path, local_directory, filename)
                if local_path:
                    downloaded_files.append(local_path)
                    
            except Exception as e:
                logger.error(f"Erro ao processar documento {doc}: {e}")
                continue
        
        return downloaded_files
    
    def find_object_in_bucket(self, bucket_name, uuid):
        """
        Procura um objeto no bucket pelo UUID, testando diferentes variações
        
        Args:
            bucket_name (str): Nome do bucket
            uuid (str): UUID do documento
            
        Returns:
            str: Nome do objeto encontrado ou None
        """
        try:
            # Listar todos os objetos do bucket
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            
            if 'Contents' not in response:
                return None
            
            objects = [obj['Key'] for obj in response['Contents']]
            
            # Procurar por diferentes variações
            candidates = [
                uuid,                    # UUID simples
                f"{uuid}.pdf",          # UUID com .pdf
                f"{uuid}.PDF",          # UUID com .PDF
                f"{uuid}.doc",          # UUID com .doc
                f"{uuid}.docx",         # UUID com .docx
            ]
            
            for candidate in candidates:
                if candidate in objects:
                    return candidate
            
            # Se não encontrou exato, procurar por substring
            for obj in objects:
                if uuid in obj:
                    logger.info(f"Encontrado objeto similar: {obj}")
                    return obj
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao procurar objeto no bucket {bucket_name}: {e}")
            return None

    def list_buckets(self):
        """
        Lista todos os buckets disponíveis
        
        Returns:
            list: Lista de nomes dos buckets
        """
        try:
            response = self.s3_client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            logger.error(f"Erro ao listar buckets: {e}")
            return []
    
    def list_objects(self, bucket_name, prefix=""):
        """
        Lista objetos em um bucket
        
        Args:
            bucket_name (str): Nome do bucket
            prefix (str): Prefixo para filtrar objetos
            
        Returns:
            list: Lista de objetos
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Erro ao listar objetos do bucket {bucket_name}: {e}")
            return []

def baixar_documento_minio(endpoint, access_key, secret_key, document_path, local_directory="downloads"):
    """
    Baixa um documento do MinIO usando as credenciais e endpoint fornecidos.

    Args:
        endpoint (str): Endpoint do MinIO (ex: 'http://localhost:9000')
        access_key (str): Chave de acesso do MinIO
        secret_key (str): Chave secreta do MinIO
        document_path (str): Path do documento na tabela
        local_directory (str): Diretório local para salvar o arquivo

    Returns:
        str: Caminho do arquivo baixado ou None se falhar
    """
    downloader = MinIODownloader(endpoint, access_key, secret_key)
    return downloader.download_document(document_path, local_directory)


def save_file_from_minio(
    file_id: str, 
    db_conn: pyodbc.Connection,
    local_directory="downloads"
) -> Optional[Path]:
    """
    Busca o caminho do arquivo no MinIO a partir do banco de dados usando o file_id,
    faz o download do documento do MinIO e retorna o caminho local do arquivo baixado.

    Parâmetros:
        file_id (str): ID do documento a ser recuperado.
        db_conn (pyodbc.Connection): Conexão ativa com o banco de dados SQL Server.

    Retorna:
        Optional[Path]: Caminho local do arquivo baixado, ou None em caso de erro.
    """
    endpoint = AppConfig.MINIO_ENDPOINT
    access_key = AppConfig.MINIO_ACCESS_KEY
    secret_key = AppConfig.MINIO_SECRET_KEY

    try:
        document_path = get_minio_file_path(file_id, db_conn)
        logger.info(f"Caminho MinIO para o documento {file_id}: {document_path}")
    except Exception as e:
        logger.error(f"Erro ao recuperar caminho do MinIO para o documento {file_id}: {e}")
        return None

    try:
        downloader = MinIODownloader(endpoint, access_key, secret_key)
        result = downloader.download_document(document_path, local_directory)
        if result:
            logger.info(f"Documento {file_id} baixado com sucesso: {result}")
            return result
        else:
            logger.error(f"Falha ao baixar documento {file_id} do MinIO.")
            return None
    except Exception as e:
        logger.error(f"Erro ao baixar documento {file_id} do MinIO: {e}")
        return None


# if __name__ == "__main__":
#     result = save_file_from_minio(
#         file_id="9332334",
#         db_conn=get_db_connection(AppConfig.SQL_SERVER_CNXN_STR)
#     )
#     print(result)


"""
# Exemplo de uso
if __name__ == "__main__":
    # Configurações do MinIO
    MINIO_ENDPOINT: str = os.getenv('MINIO_ENDPOINT')
    MINIO_ACCESS_KEY: str = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = os.getenv('MINIO_SECRET_KEY')
    
    # Inicializar downloader
    downloader = MinIODownloader(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    
    # Exemplo 1: Baixar um documento específico
    try:
        conn = get_db_connection(AppConfig.SQL_SERVER_CNXN_STR)
        doc_id = "29033388"
        caminho = get_minio_file_path(doc_id, conn)
        print(f"Caminho MinIO para o documento {doc_id}: {caminho}")
    except Exception as e:
        logger.error(f"Erro ao recuperar path get_minio_file_path: {e}")

    document_path = get_minio_file_path(doc_id, conn)

    result = downloader.download_document(document_path)
    
    if result:
        print(f"Documento baixado com sucesso: {result}")
    else:
        print("Falha ao baixar documento")

    # Exemplo 3: Listar buckets
    buckets = downloader.list_buckets()
    print(f"Buckets disponíveis: {buckets}")
    
    # Exemplo 4: Listar objetos em um bucket específico
    objects = downloader.list_objects("gampes-documento-externo")
    print(f"Objetos em gampes-documento-externo: {objects[:5]}")  # Mostrar apenas os primeiros 5

"""
