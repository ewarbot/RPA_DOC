import os
import logging
from dotenv import load_dotenv
from src.file_processor import SFTPProcessor, FileHandler, DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    load_dotenv()
    
    # Configuración de paths
    local_raw_path = os.getenv('LOCAL_RAW_PATH')
    local_processed_path = os.getenv('LOCAL_PROCESSED_PATH')
    
    # Procesar SFTP
    sftp_processor = SFTPProcessor(
        host=os.getenv('SFTP_HOST'),
        port=int(os.getenv('SFTP_PORT', 22)),
        username=os.getenv('SFTP_USER'),
        password=os.getenv('SFTP_PASSWORD'),
        remote_path=os.getenv('REMOTE_PATH'),
        local_path=local_raw_path
    )
    sftp_processor.download_files()
    
    # Manejo de archivos
    file_handler = FileHandler(
        raw_path=local_raw_path,
        processed_path=local_processed_path
    )
    file_handler.process_files()
    
    # Base de datos
    db_manager = DatabaseManager(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST')
    )
    db_manager.process_json_files(local_processed_path)

if __name__ == "__main__":
    main()