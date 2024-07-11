from etl import pipeline_extrai_calcula_kpi_carrega

pasta = 'data'
formato_saida = ['csv', 'parquet']

pipeline_extrai_calcula_kpi_carrega(pasta, formato_saida)
