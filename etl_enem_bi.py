import pandas as pd
import numpy as np


ARQUIVO_ENEM = 'MICRODADOS_ENEM_2023.csv'
QTD_LINHAS = QTD_LINHAS = 50000

print("--- INICIANDO ETL PARA POWER BI (MODELO OLAP) ---")

cols = [
    'NU_INSCRICAO', 
    'NU_NOTA_MT', 'NU_NOTA_CN', 'NU_NOTA_LC', 'NU_NOTA_CH', 'NU_NOTA_REDACAO',
    'TP_SEXO', 'TP_FAIXA_ETARIA', 'TP_COR_RACA', 'Q006', 
    'TP_ESCOLA', 
    'SG_UF_PROVA', 'NO_MUNICIPIO_PROVA' 
]

print(f"[1/5] Lendo arquivo {ARQUIVO_ENEM}...")
try:
    df = pd.read_csv(ARQUIVO_ENEM, sep=';', encoding='latin1', usecols=cols, nrows=QTD_LINHAS)
except:
    print("Tentando ler com separador vírgula...")
    df = pd.read_csv(ARQUIVO_ENEM, sep=',', encoding='latin1', usecols=cols, nrows=QTD_LINHAS)

df.dropna(subset=['NU_NOTA_MT', 'NU_NOTA_REDACAO'], how='all', inplace=True)
df.fillna(0, inplace=True) 


df['MEDIA_GERAL'] = (df['NU_NOTA_MT'] + df['NU_NOTA_CN'] + df['NU_NOTA_LC'] + df['NU_NOTA_CH'] + df['NU_NOTA_REDACAO']) / 5



regioes = {
    'Norte': ['AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC'],
    'Nordeste': ['MA', 'PI', 'CE', 'RN', 'PE', 'PB', 'SE', 'AL', 'BA'],
    'Centro-Oeste': ['MT', 'MS', 'GO', 'DF'],
    'Sudeste': ['SP', 'RJ', 'ES', 'MG'],
    'Sul': ['PR', 'RS', 'SC']
}
mapa_regiao = {uf: regiao for regiao, ufs in regioes.items() for uf in ufs}
df['REGIAO'] = df['SG_UF_PROVA'].map(mapa_regiao)


capitais = [
    'RIO BRANCO', 'MACEIO', 'MACAPA', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASILIA', 'VITORIA', 'GOIANIA',
    'SAO LUIS', 'CUIABA', 'CAMPO GRANDE', 'BELO HORIZONTE', 'BELEM', 'JOAO PESSOA', 'CURITIBA', 'RECIFE',
    'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANOPOLIS',
    'SAO PAULO', 'ARACAJU', 'PALMAS'
]

df['TIPO_MUNICIPIO'] = df['NO_MUNICIPIO_PROVA'].apply(lambda x: 'Capital' if str(x).upper() in capitais else 'Interior')


dict_raca = {0:'Não declarado', 1:'Branca', 2:'Preta', 3:'Parda', 4:'Amarela', 5:'Indígena'}
dict_escola = {1:'Não Respondeu', 2:'Pública', 3:'Privada', 4:'Exterior'}
df['RACA_DESC'] = df['TP_COR_RACA'].map(dict_raca)
df['ESCOLA_DESC'] = df['TP_ESCOLA'].map(dict_escola)

print("[2/5] Criando Tabela Dimensão: PARTICIPANTE...")
dim_participante = df[['NU_INSCRICAO', 'TP_SEXO', 'TP_FAIXA_ETARIA', 'RACA_DESC', 'Q006']].copy()
dim_participante.rename(columns={'Q006': 'CLASSE_SOCIAL'}, inplace=True)

print("[3/5] Criando Tabela Dimensão: GEOGRAFIA...")

df['CHAVE_GEO'] = df['SG_UF_PROVA'] + "-" + df['NO_MUNICIPIO_PROVA']
dim_geografia = df[['CHAVE_GEO', 'SG_UF_PROVA', 'NO_MUNICIPIO_PROVA', 'REGIAO', 'TIPO_MUNICIPIO']].drop_duplicates()

print("[4/5] Criando Tabela Fato: NOTAS...")

fato_notas = df[['NU_INSCRICAO', 'CHAVE_GEO', 'ESCOLA_DESC', 'NU_NOTA_MT', 'NU_NOTA_CN', 'NU_NOTA_LC', 'NU_NOTA_CH', 'NU_NOTA_REDACAO', 'MEDIA_GERAL']].copy()

print("[5/5] Exportando tabelas OLAP (CSV)...")
dim_participante.to_csv("DIM_Participante.csv", index=False, sep=';', encoding='utf-8-sig')
dim_geografia.to_csv("DIM_Geografia.csv", index=False, sep=';', encoding='utf-8-sig')
fato_notas.to_csv("FATO_Notas.csv", index=False, sep=';', encoding='utf-8-sig')

print("--- SUCESSO! ARQUIVOS GERADOS: FATO_Notas, DIM_Participante, DIM_Geografia ---")