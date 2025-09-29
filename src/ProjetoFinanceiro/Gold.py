from pyspak.sql import SparkSession, DataFrame


# =========================
# Config (ajuste aqui)
# =========================
USE_UNITY_CATALOG = False
CATALOG = "demo_catalog" # ignorado se USE_UNITY_CATALOG=False
SCHEMA = "fin"
DB_NO_UC = "fin" # nome do database quando NÃO usa UC

# Fontes (ligue/desligue)
ENABLE_SGS = True # BCB/SGS: Selic, CDI, IPCA...
ENABLE_PTAX = True # BCB/PTAX (exemplo simples últimos N dias)
ENABLE_CVM = True # CVM: cadastro + informes diários (ano)
ENABLE_TD = True # Tesouro Direto: taxas/preços

PTAX_LAST_N_DAYS = 30
CVM_INF_ANO = 2025

# =========================
# Setup cat/schema/database
# =========================
if USE_UNITY_CATALOG:
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")
TGT = f"{CATALOG}.{SCHEMA}"
else:
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NO_UC}")
spark.sql(f"USE {DB_NO_UC}")
TGT = DB_NO_UC

from pyspark.sql import functions as F
import datetime, requests, pandas as pd

# =========================
# 1) BCB/SGS (séries)
# =========================
if ENABLE_SGS:
series = [
{"code": 432, "name": "selic_meta"}, # meta Selic (% a.a.)
{"code": 12, "name": "cdi_diario"}, # CDI diário (% a.d.)
{"code": 433, "name": "ipca_mensal"} # IPCA var % mensal
]

def fetch_sgs(code, start, end):
url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
f"?formato=json&dataInicial={start}&dataFinal={end}")
r = requests.get(url, timeout=60); r.raise_for_status()
pdf = pd.read_json(r.text)
return spark.createDataFrame(pdf)

start = datetime.date(2000,1,1)
today = datetime.date.today()

chunks = []
cur = start
while cur <= today:
end = min(datetime.date(cur.year+9,12,31), today)
for s in series:
df = (fetch_sgs(s["code"], cur.strftime("%d/%m/%Y"), end.strftime("%d/%m/%Y"))
.withColumn("_serie_code", F.lit(s["code"]))
.withColumn("_serie_name", F.lit(s["name"]))
.withColumn("_ingest_ts", F.current_timestamp()))
chunks.append(df)
cur = end + datetime.timedelta(days=1)

bronze_sgs = chunks[0]
for df in chunks[1:]:
bronze_sgs = bronze_sgs.unionByName(df, allowMissingColumns=True)

(bronze_sgs.write.format("delta").mode("overwrite")
.saveAsTable(f"{TGT}.bronze_bcb_sgs"))

# =========================
# 2) PTAX (últimos N dias)
# =========================
if ENABLE_PTAX:
base = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
rows = []
for i in range(PTAX_LAST_N_DAYS, -1, -1):
d = (datetime.date.today() - datetime.timedelta(days=i)).strftime("%m-%d-%Y")
url = f"{base}/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{d}'&$format=json"
try:
r = requests.get(url, timeout=30); r.raise_for_status()
js = r.json().get("value", [])
rows.extend(js)
except Exception:
pass
pdf = pd.DataFrame(rows)
if not pdf.empty:
df = spark.createDataFrame(pdf) \
.withColumn("_ingest_ts", F.current_timestamp())
df.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.bronze_bcb_ptax")

# =========================
# 3) CVM – Fundos (cadastro + informe diário ano)
# =========================
if ENABLE_CVM:
cad_url = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
inf_url = f"https://dados.cvm.gov.br/dados/FI/INF_DIARIO/DADOS/inf_diario_fi_{CVM_INF_ANO}.csv"

pdf_cad = pd.read_csv(cad_url, sep=";", dtype=str, low_memory=False)
(spark.createDataFrame(pdf_cad)
.withColumn("_ingest_ts", F.current_timestamp())
.withColumn("_source_url", F.lit(cad_url))
.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.bronze_cvm_cad_fi"))

pdf_inf = pd.read_csv(inf_url, sep=";", dtype=str, low_memory=False)
(spark.createDataFrame(pdf_inf)
.withColumn("_ingest_ts", F.current_timestamp())
.withColumn("_source_url", F.lit(inf_url))
.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.bronze_cvm_inf_fi_{CVM_INF_ANO}"))

# =========================
# 4) Tesouro Direto (CSV diário)
# =========================
if ENABLE_TD:
td_csv = ("https://www.tesourotransparente.gov.br/ckan/dataset/"
"df56aa42-484a-4a59-8184-7676580c81e3/resource/"
"796d2059-14e9-44e3-80c9-2d9e30b405c1/download/precotaxatesourodireto.csv")
pdf = pd.read_csv(td_csv, sep=";", decimal=",", dtype=str)
(spark.createDataFrame(pdf)
.withColumn("_ingest_ts", F.current_timestamp())
.withColumn("_source_url", F.lit(td_csv))
.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.bronze_tesouro_direto"))

print("BRONZE ")
