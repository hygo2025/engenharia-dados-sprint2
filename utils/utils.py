import pandas as pd
import os


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
