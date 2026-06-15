# Pipeline de Dados Financeiros Brasil

## 📊 Visão Geral

Pipeline ETL completo para ingestão, transformação e análise de dados financeiros brasileiros utilizando arquitetura Medallion (Bronze → Silver → Gold) no Databricks.

## 🎯 Objetivo

Centralizar e processar dados de múltiplas fontes públicas brasileiras para análises financeiras, criando uma base consistente e confiável para dashboards, relatórios e modelos analíticos.

## 🏗️ Arquitetura

### Pipeline de Execução

**Job Databricks**: [ProjetoFinanceiro_Job](#job-85066257048496)
- **Bronze_task** → **Silver_Task** → **Gold_Task**
- Execução sequencial com dependências
- Compute: Serverless

### Arquitetura Medallion de 3 Camadas

```
┌─────────────────────────────────────────────────────────────┐
│                      FONTES EXTERNAS                        │
│  BCB/SGS │ BCB/PTAX │ CVM Fundos │ Tesouro Direto          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │   🥉 BRONZE LAYER     │
         │  (Ingestão Bruta)     │
         │ • bronze_bcb_sgs      │
         │ • bronze_bcb_ptax     │
         │ • bronze_cvm_cad_fi   │
         │ • bronze_cvm_inf_fi   │
         │ • bronze_tesouro_direto│
         └──────────┬────────────┘
                    │
                    ▼
         ┌───────────────────────┐
         │   🥈 SILVER LAYER     │
         │ (Limpeza e Padronização)│
         │ • silver_bcb_sgs      │
         │ • silver_bcb_ptax     │
         │ • silver_cvm_cad_fi   │
         │ • silver_tesouro_direto│
         └──────────┬────────────┘
                    │
                    ▼
         ┌───────────────────────┐
         │   🥇 GOLD LAYER       │
         │ (Agregações e Métricas)│
         │ • gold_macro_rates    │
         │ • gold_fx_ptax        │
         │ • gold_td_curve       │
         └──────────┬────────────┘
                    │
                    ▼
         ┌───────────────────────┐
         │   📊 DASHBOARDS       │
         │ Dashboard Financeiro  │
         └───────────────────────┘
```

## 📦 Fontes de Dados

### 1. Banco Central do Brasil (BCB)

#### BCB/SGS - Sistema Gerenciador de Séries Temporais
- **Endpoint**: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados`
- **Dados coletados**:
  - **Selic Meta** (código 432): Taxa básica de juros (% a.a.)
  - **CDI Diário** (código 12): Certificado de Depósito Interbancário (% a.d.)
  - **IPCA Mensal** (código 433): Índice de inflação (% mensal)
- **Período**: Janeiro/2000 até hoje
- **Formato**: JSON

#### BCB/PTAX - Cotações do Dólar
- **Endpoint**: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata`
- **Dados coletados**: Cotações de compra e venda do dólar (USD/BRL)
- **Período**: Últimos 30 dias (configurável)
- **Formato**: JSON (OData)

### 2. Comissão de Valores Mobiliários (CVM)

- **Cadastro de Fundos**: `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`
- **Informes Diários**: `https://dados.cvm.gov.br/dados/FI/INF_DIARIO/DADOS/inf_diario_fi_{ano}.csv`
- **Dados coletados**:
  - Cadastro completo de fundos (CNPJ, nome, classe, situação)
  - Informes diários do ano configurado (2026)
- **Formato**: CSV (separador `;`, encoding `latin1`)

### 3. Tesouro Nacional

- **Endpoint**: `https://www.tesourotransparente.gov.br/.../precotaxatesourodireto.csv`
- **Dados coletados**: Preços e taxas de todos os títulos do Tesouro Direto
- **Período**: Base histórica completa
- **Formato**: CSV (separador `;`, decimal `,`)

## 🗂️ Estrutura de Tabelas

### 🥉 Bronze Layer (Dados Brutos)

| Tabela | Descrição | Colunas Principais |
|--------|-----------|--------------------|
| `bronze_bcb_sgs` | Séries temporais BCB | data, valor, _serie_code, _serie_name |
| `bronze_bcb_ptax` | Cotações dólar | dataHoraCotacao, cotacaoCompra, cotacaoVenda |
| `bronze_cvm_cad_fi` | Cadastro fundos | CNPJ_FUNDO, DENOM_SOCIAL, CLASSE, SIT |
| `bronze_cvm_inf_fi_{ano}` | Informes diários | CNPJ_FUNDO, DT_COMPTC, VL_TOTAL, VL_QUOTA |
| `bronze_tesouro_direto` | Preços TD | Data_Base, Tipo_Titulo, PU_Compra_Manha, Taxa_Compra_Manha |

### 🥈 Silver Layer (Dados Limpos)

| Tabela | Descrição | Transformações Aplicadas |
|--------|-----------|-------------------------|
| `silver_bcb_sgs` | Séries BCB padronizadas | • Conversão data (dd/MM/yyyy → date)<br>• Conversão valor (string → double)<br>• Padronização decimal (`,` → `.`) |
| `silver_bcb_ptax` | FX padronizado | • Extração data do timestamp<br>• Conversão cotações para double |
| `silver_cvm_cad_fi` | Cadastro limpo | • Remoção formatação CNPJ<br>• Seleção colunas relevantes |
| `silver_tesouro_direto` | TD estruturado | • Conversão datas<br>• Padronização decimais<br>• Renomeação colunas consistentes |

### 🥇 Gold Layer (Dados Analíticos)

| Tabela | Descrição | Métricas / Dimensões |
|--------|-----------|---------------------|
| `gold_macro_rates` | Indicadores macro pivotados | • data (date)<br>• selic_meta (double)<br>• cdi_diario (double)<br>• ipca_mensal (double) |
| `gold_fx_ptax` | FX agregado com variação | • data (date)<br>• ptax_compra, ptax_venda (médias)<br>• ptax_mid (ponto médio)<br>• var_d (variação diária %) |
| `gold_td_curve` | Curva sintética Tesouro | • data, TipoTitulo<br>• y_buy, y_sell (yields médios)<br>• pu_buy, pu_sell (PUs médios) |

## 🚀 Como Usar

### Pré-requisitos

- Databricks Workspace (CPU ou GPU serverless)
- Acesso à internet para APIs públicas
- Python 3.x com bibliotecas: `requests`, `pandas`, `pyspark`

### Configuração

Edite as configurações no topo de cada notebook:

```python
# =========================
# Config (ajuste aqui)
# =========================
USE_UNITY_CATALOG = False      # True para usar Unity Catalog
CATALOG = "demo_catalog"       # Nome do catálogo (se UC ativado)
SCHEMA = "fin"                 # Schema/database
DB_NO_UC = "fin"               # Database quando não usa UC

# Fontes (ligue/desligue)
ENABLE_SGS  = True             # BCB/SGS: Selic, CDI, IPCA
ENABLE_PTAX = True             # BCB/PTAX
ENABLE_CVM  = True             # CVM: fundos
ENABLE_TD   = True             # Tesouro Direto

# Parâmetros
PTAX_LAST_N_DAYS = 30          # Janela de dias PTAX
CVM_INF_ANO      = 2026        # Ano dos informes CVM
```

### Execução do Pipeline

#### Ordem de Execução (importante!)

1. **Bronze Layer** (`Camada Bronze - Anderson Teste`)
   ```python
   # Executa ingestão de todas as fontes habilitadas
   # Duração estimada: 5-10 minutos
   ```

2. **Silver Layer** (notebook Silver)
   ```python
   # Executa limpeza e padronização
   # Duração estimada: 2-3 minutos
   ```

3. **Gold Layer** (`Camada Gold 2025-11-25 18:29:53`)
   ```python
   # Cria tabelas agregadas
   # Duração estimada: 1-2 minutos
   ```

#### Execução Manual

Execute os notebooks na ordem acima. Cada notebook imprime confirmação ao final:
- Bronze: `"BRONZE OK"`
- Silver: `"SILVER "`
- Gold: `"GOLD "`

## 📊 Dashboard

### Dashboard Financeiro

Dashboard interativo com visualizações dos dados processados:

#### Filtros Globais
- **Ano**: Filtra por ano (2004-2026)
- **Mês**: Filtra por mês (1-12)
- **Dia**: Filtra por dia (1-31)

#### Visualizações

1. **Indicadores Macroeconômicos**
   - Contadores: CDI, IPCA, Selic (valores mais recentes)
   - Gráfico de linha: Evolução temporal dos 3 indicadores

2. **Tesouro Direto**
   - Gráfico de linha: Tendência dos yields por tipo de título
   - Gráfico de barras: Comparação de yields entre títulos
   - Tabela: Taxas mais recentes por título
   - Contador: Yield médio de compra
   - Tabela completa: Histórico completo da curva

## 💡 Exemplos de Queries

### Consultar últimas taxas macro

```sql
SELECT *
FROM workspace.fin.gold_macro_rates
ORDER BY data DESC
LIMIT 10
```

### Analisar volatilidade do dólar

```sql
SELECT 
  data,
  ptax_mid,
  var_d * 100 AS variacao_pct,
  CASE 
    WHEN ABS(var_d) > 0.02 THEN 'Alta Volatilidade'
    WHEN ABS(var_d) > 0.01 THEN 'Média Volatilidade'
    ELSE 'Baixa Volatilidade'
  END AS classificacao
FROM workspace.fin.gold_fx_ptax
WHERE data >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY data DESC
```

### Comparar yields do Tesouro

```sql
SELECT 
  TipoTitulo,
  AVG(y_buy) AS yield_medio,
  MIN(y_buy) AS yield_min,
  MAX(y_buy) AS yield_max,
  STDDEV(y_buy) AS yield_volatilidade
FROM workspace.fin.gold_td_curve
WHERE data >= '2025-01-01'
GROUP BY TipoTitulo
ORDER BY yield_medio DESC
```

### Correlação Selic x CDI

```sql
SELECT 
  data,
  selic_meta,
  cdi_diario,
  selic_meta - cdi_diario AS spread
FROM workspace.fin.gold_macro_rates
WHERE data >= '2024-01-01'
  AND selic_meta IS NOT NULL
  AND cdi_diario IS NOT NULL
ORDER BY data
```

## 🛠️ Manutenção e Troubleshooting

### Tratamento de Erros

O pipeline inclui tratamento de erros robusto:

```python
# Exemplo: BCB/SGS com retry em caso de JSON inválido
try:
    return pd.read_json(r.text)
except ValueError:
    print(f"Warning: Invalid JSON for series {code} ({start} to {end})")
    return pd.DataFrame()
```

### Problemas Comuns

| Problema | Causa | Solução |
|----------|-------|--------|
| JSONDecodeError | API retornou HTML/erro | Já tratado com try-except; verifica warnings |
| Tabela não existe | Silver executou antes Bronze | Execute na ordem: Bronze → Silver → Gold |
| Timeout API | Rede lenta/instável | Aumentar timeout em `requests.get(url, timeout=60)` |
| Encoding error CVM | Arquivo em latin1 | Já configurado: `encoding="latin1"` |
| PTAX sem dados | Finais de semana/feriados | Normal; API não retorna dados nesses dias |

### Logs e Monitoramento

- Cada camada imprime confirmação ao final da execução
- Warnings para dados inválidos são impressos no console
- Coluna `_ingest_ts` em todas as tabelas bronze/silver registra timestamp de ingestão

## 📝 Estrutura de Arquivos

```
/Workspace/Users/anderson.dbm@gmail.com/
├── README.md                         # Este arquivo
├── Camada Bronze.ipynb               # Ingestão bruta
├── Camada Silver.ipynb               # Limpeza (nome inferido)
├── Camada Gold.ipynb                 # Agregações
└── Dashboard Financeiro.lvdash.json  # Dashboard
```

## 🤝 Contribuindo

Este é um projeto pessoal de estudo. Sugestões de melhorias:

1. Adicionar novas fontes de dados
2. Implementar testes automatizados
3. Otimizar queries SQL
4. Criar novos dashboards

## 📄 Licença

Projeto educacional. Dados públicos fornecidos por:
- Banco Central do Brasil
- Comissão de Valores Mobiliários
- Tesouro Nacional

## 📞 Contato

**Autor**: Anderson  
**Email**: anderson.dbm@gmail.com  
**Workspace**: Databricks

---

**Última atualização**: Junho 2026  
**Versão**: 1.0
