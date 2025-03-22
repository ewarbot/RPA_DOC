import os
import logging
import sys
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
    
    try:
        # Procesar SFTP
        sftp_processor = SFTPProcessor(
            host=os.getenv('SFTP_HOST'),
            port=int(os.getenv('SFTP_PORT', 22)),
            username=os.getenv('SFTP_USER'),
            password=os.getenv('SFTP_PASSWORD'),
            remote_path=os.getenv('REMOTE_PATH'),
            local_path=local_raw_path
        )

        # Verificar conexión antes de descargar
        if not sftp_processor.test_connection():
            logging.error("❌ Fallo en la conexión SFTP. Verifica:")
            logging.error(f"  Host: {os.getenv('SFTP_HOST')}")
            logging.error(f"  Usuario: {os.getenv('SFTP_USER')}")
            logging.error(f"  Puerto: {os.getenv('SFTP_PORT', 22)}")
            logging.error("  Contraseña: [Configurada]" if os.getenv('SFTP_PASSWORD') else "  Contraseña: No configurada")
            sys.exit(1)

        logging.info("✅ Conexión SFTP establecida correctamente")
        downloaded_files = sftp_processor.download_files()
        
        if not downloaded_files:
            logging.info("No se encontraron archivos nuevos para descargar")
            return

        logging.info(f"📥 Archivos descargados ({len(downloaded_files)}):")
        for file in downloaded_files:
            logging.info(f"  - {file}")

        # Manejo de archivos
        file_handler = FileHandler(
            raw_path=local_raw_path,
            processed_path=local_processed_path
        )
        processed_files = file_handler.process_files()
        
        if not processed_files:
            logging.info("No se encontraron archivos para procesar")
            return

        logging.info(f"🔄 Archivos procesados ({len(processed_files)}):")
        for file in processed_files:
            logging.info(f"  - {file}")

        # Base de datos
        db_manager = DatabaseManager(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST')
        )
        db_manager.process_json_files(local_processed_path)
        logging.info("💾 Datos guardados en PostgreSQL exitosamente")

    except Exception as e:
        logging.error(f"🚨 Error crítico en el proceso: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()