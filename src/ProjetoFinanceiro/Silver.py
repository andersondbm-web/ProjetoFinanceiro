from pyspark.sql import SparkSession, DataFrame


# =========================
# Config
# =========================
USE_UNITY_CATALOG = False
CATALOG = "demo_catalog"
SCHEMA = "fin"
DB_NO_UC = "fin"
if USE_UNITY_CATALOG:
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE {SCHEMA}"); TGT=f"{CATALOG}.{SCHEMA}"
else:
spark.sql(f"USE {DB_NO_UC}"); TGT=DB_NO_UC

from pyspark.sql import functions as F

# ---- SGS
sgs_bz = spark.table(f"{TGT}.bronze_bcb_sgs")
sgs_sv = (sgs_bz
.withColumn("data", F.to_date("data","dd/MM/yyyy"))
.withColumn("valor", F.regexp_replace("valor", ",", ".").cast("double"))
.select("data","valor","_serie_code","_serie_name","_ingest_ts"))
sgs_sv.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.silver_bcb_sgs")

# ---- PTAX
if f"{TGT}.bronze_bcb_ptax" in [t.name for t in spark.catalog.listTables()]:
ptx_bz = spark.table(f"{TGT}.bronze_bcb_ptax")
# campos variam conforme retorno; normalização defensiva
cols = ptx_bz.columns
compra = "cotacaoCompra" if "cotacaoCompra" in cols else "CotacaoCompra"
venda = "cotacaoVenda" if "cotacaoVenda" in cols else "CotacaoVenda"
datahr = "dataHoraCotacao" if "dataHoraCotacao" in cols else "DataHoraCotacao"

ptx_sv = (ptx_bz
.withColumn("data", F.to_date(datahr))
.withColumn("compra", F.col(compra).cast("double"))
.withColumn("venda", F.col(venda).cast("double"))
.select("data","compra","venda","_ingest_ts"))
ptx_sv.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.silver_bcb_ptax")

# ---- CVM: cadastro
cad = spark.table(f"{TGT}.bronze_cvm_cad_fi")
cad_sv = (cad
.withColumn("CNPJ_FUNDO", F.regexp_replace("CNPJ_FUNDO", r"[^\d]", ""))
.select("CNPJ_FUNDO","DENOM_SOCIAL","FUNDO_CLASSE","SIT","_ingest_ts"))
cad_sv.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.silver_cvm_cad_fi")

# ---- CVM: informes (ajuste ano conforme Bronze)
# tente detectar automaticamente (pega 1 tabela que comece com bronze_cvm_inf_fi_)
inf_tbl = [t.name for t in spark.catalog.listTables() if t.name.startswith("bronze_cvm_inf_fi_")]
assert len(inf_tbl)>0, "Tabela bronze_cvm_inf_fi_XXXX não encontrada."
inf_bz = spark.table(f"{TGT}.{inf_tbl[0]}")

inf_sv = (inf_bz
.withColumn("CNPJ_FUNDO", F.regexp_replace("CNPJ_FUNDO", r"[^\d]", ""))
.withColumn("DT_COMPTC", F.to_date("DT_COMPTC","yyyy-MM-dd"))
.withColumn("VL_QUOTA", F.regexp_replace("VL_QUOTA", ",", ".").cast("double"))
.withColumn("VL_PATRIM_LIQ", F.regexp_replace("VL_PATRIM_LIQ", ",", ".").cast("double"))
.select("CNPJ_FUNDO","DT_COMPTC","VL_QUOTA","VL_PATRIM_LIQ","_ingest_ts"))
inf_sv.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.silver_cvm_inf_fi")

# ---- Tesouro Direto
td_bz = spark.table(f"{TGT}.bronze_tesouro_direto")
td_sv = (td_bz
.withColumn("data", F.to_date("DataBase","dd/MM/yyyy"))
.withColumn("TipoTitulo", F.col("TipoTitulo"))
.withColumn("VencimentoTitulo", F.col("VencimentoTitulo"))
.withColumn("PUCompra", F.regexp_replace("PUCompra", ",", ".").cast("double"))
.withColumn("PUVenda", F.regexp_replace("PUVenda", ",", ".").cast("double"))
.withColumn("TaxaCompra", F.regexp_replace("TaxaCompra", ",", ".").cast("double"))
.withColumn("TaxaVenda", F.regexp_replace("TaxaVenda", ",", ".").cast("double"))
.select("data","TipoTitulo","VencimentoTitulo","PUCompra","PUVenda","TaxaCompra","TaxaVenda","_ingest_ts"))
td_sv.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.silver_tesouro_direto")

print("SILVER ")
