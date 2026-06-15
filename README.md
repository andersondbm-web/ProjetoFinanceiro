Projeto Finance Data Lake Brasil
Visão Geral

Este projeto implementa um pipeline de ingestão e transformação de dados financeiros brasileiros utilizando arquitetura Medallion (Bronze, Silver e Gold) em Apache Spark/Databricks com armazenamento Delta Lake.

O pipeline coleta dados de fontes públicas oficiais, realiza tratamentos e disponibiliza datasets analíticos para consumo por dashboards, modelos analíticos e aplicações de dados.

Fontes de Dados
Fonte	Descrição
Banco Central do Brasil (SGS)	Séries temporais econômicas (Selic, CDI, IPCA)
Banco Central do Brasil (PTAX)	Cotações oficiais de câmbio
CVM	Cadastro e informes diários de fundos de investimento
Tesouro Direto	Taxas e preços dos títulos públicos
Arquitetura
                +------------------+
                | Fontes Externas  |
                +--------+---------+
                         |
       +-----------------+-----------------+
       |                 |                 |
     SGS              PTAX              CVM
       |                 |                 |
       +-----------------+-----------------+
                         |
                    Tesouro
                     Direto
                         |
                         v

                 =================
                 BRONZE LAYER
                 =================

          bronze_bcb_sgs
          bronze_bcb_ptax
          bronze_cvm_cad_fi
          bronze_cvm_inf_fi_YYYY
          bronze_tesouro_direto

                         |
                         v

                 =================
                 SILVER LAYER
                 =================

          silver_bcb_sgs
          silver_bcb_ptax
          silver_cvm_cad_fi
          silver_tesouro_direto

                         |
                         v

                 =================
                 GOLD LAYER
                 =================

          gold_macro_rates
          gold_fx_ptax
          gold_td_curve
Estrutura do Projeto
project/
│
├── bronze/
│   └── ingestao_fontes.py
│
├── silver/
│   └── transformacoes.py
│
├── gold/
│   └── agregacoes.py
│
└── README.md
Camada Bronze

A camada Bronze é responsável pela ingestão dos dados brutos exatamente como disponibilizados pelas fontes.

Tabelas
bronze_bcb_sgs

Armazena séries temporais do Sistema Gerenciador de Séries (SGS).

Séries carregadas:

Código	Série
432	Meta Selic
12	CDI Diário
433	IPCA Mensal

Campos adicionados:

_serie_code
_serie_name
_ingest_ts
bronze_bcb_ptax

Armazena cotações PTAX dos últimos N dias.

Campos principais:

cotacaoCompra
cotacaoVenda
dataHoraCotacao
bronze_cvm_cad_fi

Cadastro completo dos fundos de investimento registrados na CVM.

bronze_cvm_inf_fi_YYYY

Informe diário dos fundos de investimento para o ano configurado.

bronze_tesouro_direto

Dados de preços e taxas dos títulos públicos negociados no Tesouro Direto.

Camada Silver

A camada Silver realiza limpeza, padronização e tipagem dos dados.

silver_bcb_sgs

Transformações:

Conversão de data para DATE
Conversão de valor para DOUBLE
Padronização das colunas

Estrutura:

data
valor
_serie_code
_serie_name
_ingest_ts
silver_bcb_ptax

Transformações:

Conversão das datas
Conversão das cotações para DOUBLE

Estrutura:

data
compra
venda
_ingest_ts
silver_cvm_cad_fi

Transformações:

Limpeza do CNPJ
Seleção das colunas relevantes

Estrutura:

CNPJ_FUNDO
DENOM_SOCIAL
CLASSE
SIT
_ingest_ts
silver_tesouro_direto

Transformações:

Conversão de datas
Conversão de taxas e preços para DOUBLE
Padronização de nomes

Estrutura:

data
TipoTitulo
VencimentoTitulo
PUCompra
PUVenda
TaxaCompra
TaxaVenda
_ingest_ts
Camada Gold

A camada Gold disponibiliza datasets prontos para consumo analítico.

gold_macro_rates

Tabela de indicadores macroeconômicos consolidados.

Estrutura:

data
selic_meta
cdi_diario
ipca_mensal

Utiliza pivot da tabela Silver SGS.

Exemplo:

Data	Selic	CDI	IPCA
2026-01-01	15.00	0.05	0.42
gold_fx_ptax

Tabela de análise cambial.

Métricas:

PTAX Compra
PTAX Venda
PTAX Média
Variação Diária

Estrutura:

data
ptax_compra
ptax_venda
ptax_mid
var_d
gold_td_curve

Curva sintética do Tesouro Direto.

Agregações:

Taxa média de compra
Taxa média de venda
PU médio de compra
PU médio de venda

Estrutura:

data
TipoTitulo
y_buy
y_sell
pu_buy
pu_sell
Configurações
Unity Catalog
USE_UNITY_CATALOG = True
CATALOG = "demo_catalog"
SCHEMA = "fin"
Sem Unity Catalog
USE_UNITY_CATALOG = False
DB_NO_UC = "fin"
Fontes Habilitadas
ENABLE_SGS  = True
ENABLE_PTAX = True
ENABLE_CVM  = True
ENABLE_TD   = True
Execução
1. Executar Bronze

Responsável pela ingestão dos dados externos.

# notebook bronze

Resultado:

BRONZE OK
2. Executar Silver

Responsável pela limpeza e padronização.

# notebook silver

Resultado:

SILVER
3. Executar Gold

Responsável pelas agregações analíticas.

# notebook gold

Resultado:

GOLD
