import pandas as pd
import os
import glob

# funcao de extract, le e consolida os arquivos json
def extract_and_consolidate_data(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# funcao de transform
def calculate_sales_kpi(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

# funcao de load em csv ou parquet
def load_data(df: pd.DataFrame, formato_saida: list):
    """
    o parametro formato_saida pode ser 'csv', 'parquet' ou os dois
    """
    for formato in formato_saida:
        if formato == 'csv':
            df.to_csv('data_processed/dados.csv', index = False)
        if formato == 'parquet':
            df.to_parquet('data_processed/dados.parquet', index = False)


def pipeline_extrai_calcula_kpi_carrega(pasta: str, formato_saida: list ):
    data_frame = extract_and_consolidate_data(pasta)
    data_frame_calculado = calculate_sales_kpi(data_frame)
    load_data(data_frame_calculado, formato_saida)