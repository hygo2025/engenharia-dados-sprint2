import pandas as pd
import os
import boto3
from botocore.exceptions import NoCredentialsError


class Utils:
    @staticmethod
    def merge_csv_files(input_folder, output_file):
        directory = os.path.dirname(output_file)
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        # Listar todos os arquivos CSV no diretório de entrada
        csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

        # Ler e juntar todos os arquivos CSV
        df_list = [pd.read_csv(os.path.join(input_folder, file)) for file in csv_files]
        merged_df = pd.concat(df_list, ignore_index=True)

        # Salvar o DataFrame resultante em um novo arquivo CSV
        merged_df.to_csv(output_file, index=False)
        print(f'Arquivos CSV combinados em {output_file}')

    @staticmethod
    def upload_to_s3(file_name, bucket, object_name):
        # Inicializar a sessão do S3
        s3_client = boto3.client('s3')

        try:
            # Fazer upload do arquivo para o bucket especificado
            s3_client.upload_file(file_name, bucket, object_name)
            print(f'Arquivo {file_name} carregado com sucesso para {bucket}/{object_name}')
        except NoCredentialsError:
            print('Credenciais não disponíveis')

    @staticmethod
    def upload_all_csv_files(input_folder, bucket, base_path, folder_name):
        # Listar todos os arquivos CSV no diretório de entrada
        csv_files = [f for f in os.listdir(f"{input_folder}/{folder_name}") if f.endswith('.csv')]

        # Fazer upload de cada arquivo CSV
        for file in csv_files:
            file_path = os.path.join(input_folder, folder_name, file)
            object_name = f"{base_path}/{folder_name}/{file}"
            Utils.upload_to_s3(file_path, bucket, object_name)