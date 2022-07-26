# Biblioteca para fazer requisições HTTP
import requests

# Biblioteca para trabalhar manipulação de dados
import pandas as pd

# Biblioteca para trabalhar operações matemáticas e matriciais
import numpy as np

# Consultar, baixar e extrair os dados do site

# Variaveis para base de consulta
URL_BASE = 'http://transparencia.al.gov.br/pessoal'
ANO = 2021
MES = 10
LIMITE = 10
OFFSET = 0


def get_dados_servidores_ativos(URL_BASE, ANO, MES, LIMITE, OFFSET):
    '''
        Função para obter os datos de todos os servidores ativos do site do Estado de Alagoas via API
    '''
    URL_CONSULTA = f'{URL_BASE}/json-servidores/?ano={ANO}&mes={MES}&limit={LIMITE}&offset={OFFSET}'
    # print(URL_CONSULTA)

    # Faz a requisição HTTP
    response = requests.get(URL_CONSULTA)
    # Transforma a resposta em um dicionário
    dados = response.json()
    # Retorna o dicionário
    return dados

def get_dados_servidor(URL_BASE, id_servidor):
    '''
        Função para obter os dados do site do Estado de Alagoas
    '''
    URL_FINAL = f'{URL_BASE}/json-perfil-servidor/{id_servidor}/?limit=1&offset=0'
    # print(URL_FINAL)
    
    # Faz a requisição HTTP
    response = requests.get(URL_FINAL)
    # Transforma a resposta em um dicionário
    dados = response.json()
    # Retorna o dicionário
    return dados


# Realiza a consulta dos dados e recebe um json
json_servidores = get_dados_servidores_ativos(URL_BASE, ANO, MES, LIMITE, OFFSET)

# Criar um dataframe com os dados de rows. Porque os dados de interesse estão no rows.
df_servidores = pd.DataFrame(json_servidores['rows'])

# Imprimir a quantidade de servidores ativos
print(f'Quantidade de servidores ativos: {len(df_servidores)}')

# Criar as chaves para o dataframe de interesse
chaves_dataframe_final = ['ano', 'mes', 'numero_ordem', 'codigo_orgao', 'descricao_orgao', 'matricula', 'nome', 'cpf', 'vinculo', 'cargo', 'funcao', 'remuneracao_base', 'beneficios', 'eventuais', 'horas_extras', 'judiciais', 'comissao', 'teto_redutor', 'irrf', 'contribuicao_previdenciaria', 'total']
# Criar um dataframe vazio com as chaves de interesse
df_servidores_final = pd.DataFrame(columns=chaves_dataframe_final)

# Filtrar os dados para montar o dataframe final
for i in range(len(df_servidores)):
    # Obter o id do servidor
    id_servidor = df_servidores.iloc[i]['funcionario_id']
    
    # Obter os dados do servidor pelo dataframe
    servidor_atual = df_servidores.iloc[i]

    # print(f'Obtendo dados do servidor {id_servidor}')
    # Obter os dados do servidor
    json_servidor = get_dados_servidor(URL_BASE, id_servidor)

    # Criar dataframes com os dados do servidor. Detalhes e rows.
    df_servidor_detalhes = pd.DataFrame(json_servidor['detalhe'], index=[0])
    df_servidor_rows = pd.DataFrame(json_servidor['rows'])

    # Filtragem e limpeza dos dados para o formato do dataframe final
    # Criar um dicionário com os as chaves e os valores
    ano_mes =  df_servidor_rows.iloc[0]['ano_mes'].split('/')
    ano = int(ano_mes[0])
    mes = int(ano_mes[1])

    vinculo = df_servidor_detalhes.iloc[0]['vinculo']
    cargo = df_servidor_detalhes.iloc[0]['cargo']
    funcao =  df_servidor_detalhes.iloc[0]['funcao']

    # Replace para converter o valor da remuneração base para float
    remuneracao_base = (df_servidor_rows.iloc[0]['remuneracao_base']).replace('.', '').replace(',', '.')
    beneficios = (df_servidor_rows.iloc[0]['beneficios']).replace('.', '').replace(',', '.')
    eventuais = (df_servidor_rows.iloc[0]['eventuais']).replace('.', '').replace(',', '.')
    horas_extras = (df_servidor_rows.iloc[0]['horas_extras']).replace('.', '').replace(',', '.')
    judiciais = (df_servidor_rows.iloc[0]['judiciais']).replace('.', '').replace(',', '.')
    comissao = (df_servidor_rows.iloc[0]['comissao']).replace('.', '').replace(',', '.')
    teto_redutor = (df_servidor_rows.iloc[0]['teto_redutor']).replace('.', '').replace(',', '.')
    irrf = (df_servidor_rows.iloc[0]['irrf']).replace('.', '').replace(',', '.')
    total = (df_servidor_rows.iloc[0]['total']).replace('.', '').replace(',', '.')
    contribuicao_previdenciaria = (df_servidor_rows.iloc[0]['contribuicao_previdenciaria']).replace('.', '').replace(',', '.')
    
    # Montar o dicionário com os dados do servidor que serão inseridos no dataframe final
    df_linha_dados_servidor = pd.DataFrame({'ano': [ano], 'mes': [mes], 'numero_ordem': [servidor_atual['numero_ordem']],
                    'codigo_orgao': [servidor_atual['codigo_orgao']], 'descricao_orgao': [servidor_atual['descricao_orgao']],
                    'matricula': [servidor_atual['id']], 'nome': [servidor_atual['nome']], 'cpf': [servidor_atual['cpf']],
                    'vinculo': [vinculo], 'cargo': [cargo], 'funcao': [funcao],
                    'remuneracao_base': [remuneracao_base], 'beneficios': [beneficios], 'eventuais': [eventuais],
                    'horas_extras': [horas_extras], 'judiciais': [judiciais], 'comissao': [comissao],
                    'teto_redutor': [teto_redutor], 'irrf': [irrf], 'contribuicao_previdenciaria': [contribuicao_previdenciaria],
                    'total': [total]})
    # Adicionar o dicionário ao dataframe df_servidores_final
    df_servidores_final = pd.concat([df_servidores_final, df_linha_dados_servidor])
    
    # Aguardar um pouco para não sobrecarregar o servidor
    # time.sleep(1)

# Pré-processamento dos dados no dataframe para formatar cada coluna de forma correta
df_servidores_final['ano'] = df_servidores_final['ano'].astype(np.int64)
df_servidores_final['mes'] = df_servidores_final['mes'].astype(np.int64)
df_servidores_final['numero_ordem'] = df_servidores_final['numero_ordem'].astype(object)
df_servidores_final['codigo_orgao'] = df_servidores_final['codigo_orgao'].astype(object)
df_servidores_final['descricao_orgao'] = df_servidores_final['descricao_orgao'].astype(object)
df_servidores_final['matricula'] = df_servidores_final['matricula'].astype(object)
df_servidores_final['nome'] = df_servidores_final['nome'].astype(object)
df_servidores_final['cpf'] = df_servidores_final['cpf'].astype(object)
df_servidores_final['vinculo'] = df_servidores_final['vinculo'].astype(object)
df_servidores_final['cargo'] = df_servidores_final['cargo'].astype(object)
df_servidores_final['funcao'] = df_servidores_final['funcao'].astype(object)
df_servidores_final['remuneracao_base'] = df_servidores_final['remuneracao_base'].astype(np.float64)
df_servidores_final['beneficios'] = df_servidores_final['beneficios'].astype(np.float64)
df_servidores_final['eventuais'] = df_servidores_final['eventuais'].astype(np.float64)
df_servidores_final['horas_extras'] = df_servidores_final['horas_extras'].astype(np.float64)
df_servidores_final['judiciais'] = df_servidores_final['judiciais'].astype(np.float64)
df_servidores_final['comissao'] = df_servidores_final['comissao'].astype(np.float64)
df_servidores_final['teto_redutor'] = df_servidores_final['teto_redutor'].astype(np.float64)
df_servidores_final['irrf'] = df_servidores_final['irrf'].astype(np.float64)
df_servidores_final['contribuicao_previdenciaria'] = df_servidores_final['contribuicao_previdenciaria'].astype(np.float64)
df_servidores_final['total'] = df_servidores_final['total'].astype(np.float64)

# Apresentar os dados
print(df_servidores_final.head())
print(df_servidores_final.info())
print(df_servidores_final.describe())

# Salvar em um arquivo .csv
df_servidores_final.to_csv(f'{ANO}_{MES}_servidores_ativos.csv', index=False, encoding='utf-8')