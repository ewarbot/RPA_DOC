import os
import json
import logging
import paramiko
import time
from dotenv import load_dotenv
from datetime import datetime
import rarfile
import psycopg2
from psycopg2.extras import execute_batch
from config.parser_config import (
    PARSERS_CONFIG,
    FILE_PATTERNS,
    CUSTOM_PARSERS,
    DATA_TYPE_MAP
)
import re


class SFTPProcessor:
    def __init__(self, host, port, username, password, remote_path, local_path):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_path = remote_path
        self.local_path = local_path
        self.transport = None
        self.sftp = None

    def connect(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def download_files(self):
        try:
            self.connect()
            files = self.sftp.listdir(self.remote_path)
            for file in files:
                if file.endswith('.rar'):
                    remote_file = os.path.join(self.remote_path, file)
                    local_file = os.path.join(self.local_path, file)
                    self.sftp.get(remote_file, local_file)
                    logging.info(f'Downloaded {file}')
                    self.sftp.remove(remote_file)
                    logging.info(f'Deleted remote file {file}')
        except Exception as e:
            logging.error(f'Error in SFTP processing: {str(e)}')
        finally:
            if self.sftp: self.sftp.close()
            if self.transport: self.transport.close()

class FileHandler:
    def __init__(self, raw_path, processed_path):
        self.raw_path = raw_path
        self.processed_path = processed_path
        os.makedirs(raw_path, exist_ok=True)
        os.makedirs(processed_path, exist_ok=True)

    def extract_rars(self):
        for file in os.listdir(self.raw_path):
            if file.endswith('.rar'):
                rar_path = os.path.join(self.raw_path, file)
                with rarfile.RarFile(rar_path) as rf:
                    rf.extractall(self.raw_path)
                os.remove(rar_path)
                logging.info(f'Extracted and removed {file}')

    def process_files(self):
        self.extract_rars()
        # Aquí agregar lógica de parseo específica
        for file in os.listdir(self.raw_path):
            if file.endswith('.txt'):
                txt_path = os.path.join(self.raw_path, file)
                json_data = self.parse_txt(txt_path)
                json_file = os.path.join(self.processed_path, f'{file}.json')
                with open(json_file, 'w') as f:
                    json.dump(json_data, f)
                os.remove(txt_path)

    def determine_file_type(self, filename):
        for file_type, pattern in FILE_PATTERNS.items():
            if re.match(pattern, filename):
                return file_type
        return None

    def parse_txt(self, file_path):
        filename = os.path.basename(file_path)
        file_type = self.determine_file_type(filename)
        
        if not file_type:
            raise ValueError(f"No hay parser configurado para {filename}")
            
        config = PARSERS_CONFIG[file_type]
        data = []
        
        with open(file_path, 'r', encoding=config.get('encoding', 'utf-8')) as f:
            # Saltar líneas iniciales si está configurado
            for _ in range(config.get('skip_lines', 0)):
                next(f)
            
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Procesamiento según tipo de archivo
                if 'delimiter' in config:
                    fields = line.split(config['delimiter'])
                elif 'fixed_width' in config:
                    fields = [
                        line[start:end].strip() 
                        for start, end in config['fixed_width']
                    ]
                
                # Crear diccionario con los campos
                record = {}
                for field_name, value in zip(config['fields'], fields):
                    # Aplicar conversiones de tipo
                    data_type = DATA_TYPE_MAP.get(field_name, str)
                    
                    # Aplicar parser personalizado si existe
                    if field_name in CUSTOM_PARSERS:
                        value = CUSTOM_PARSERS[field_name](value)
                    else:
                        try:
                            value = data_type(value)
                        except:
                            value = value.strip()
                    
                    record[field_name] = value
                
                data.append(record)
        
        return data

class DatabaseManager:
    def __init__(self, dbname, user, password, host, retries=5, delay=5):
        for attempt in range(retries):
            try:
                self.conn = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host
                )
                break
            except psycopg2.OperationalError as e:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
        self.create_table()

    def create_table(self):
        with self.conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS processed_data (
                    id SERIAL PRIMARY KEY,
                    filename VARCHAR(255),
                    data JSONB,
                    processed_at TIMESTAMP
                )
            ''')
            self.conn.commit()

    def process_json_files(self, path):
        for file in os.listdir(path):
            if file.endswith('.json'):
                file_path = os.path.join(path, file)
                with open(file_path) as f:
                    data = json.load(f)
                    self.save_to_db(file, data)
                os.remove(file_path)

    def save_to_db(self, filename, data):
        with self.conn.cursor() as cur:
            execute_batch(cur,
                '''INSERT INTO processed_data 
                (filename, data, processed_at)
                VALUES (%s, %s, %s)''',
                [(filename, json.dumps(data), datetime.now())]
            )
            self.conn.commit()