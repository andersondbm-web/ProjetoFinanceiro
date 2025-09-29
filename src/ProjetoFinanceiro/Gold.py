from pyspak.sql import SparkSession, DataFrame


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

from pyspark.sql import functions as F, Window

# ---- Macro (pivot SGS)
sgs = spark.table(f"{TGT}.silver_bcb_sgs")
macro = (sgs.groupBy("data")
.pivot("_serie_name")
.agg(F.first("valor"))
.orderBy("data"))
macro.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.gold_macro_rates")

# ---- FX (PTAX)
if f"{TGT}.silver_bcb_ptax" in [t.name for t in spark.catalog.listTables()]:
ptx = spark.table(f"{TGT}.silver_bcb_ptax")
w = Window.orderBy("data")
fx = (ptx.groupBy("data")
.agg(F.avg("compra").alias("ptax_compra"),
F.avg("venda").alias("ptax_venda"))
.withColumn("ptax_mid",(F.col("ptax_compra")+F.col("ptax_venda"))/2)
.withColumn("var_d", (F.col("ptax_mid")/F.lag("ptax_mid").over(w)-1)))
fx.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.gold_fx_ptax")

# ---- Tesouro Direto (curva sintética)
td = spark.table(f"{TGT}.silver_tesouro_direto")
td_curve = (td.groupBy("data","TipoTitulo")
.agg(F.avg("TaxaCompra").alias("y_buy"),
F.avg("TaxaVenda").alias("y_sell"),
F.avg("PUCompra").alias("pu_buy"),
F.avg("PUVenda").alias("pu_sell")))
td_curve.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.gold_td_curve")

# ---- Fundos: AUM & painel
cad = spark.table(f"{TGT}.silver_cvm_cad_fi")
inf = spark.table(f"{TGT}.silver_cvm_inf_fi")

aum = (inf.groupBy("DT_COMPTC")
.agg(F.sum("VL_PATRIM_LIQ").alias("aum_total"),
F.countDistinct("CNPJ_FUNDO").alias("n_fundos")))
aum.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.gold_fi_aum_diario")

fi_painel = (inf.join(cad, "CNPJ_FUNDO","left")
.select(F.col("DT_COMPTC").alias("data"),
"CNPJ_FUNDO","DENOM_SOCIAL","FUNDO_CLASSE",
"VL_QUOTA","VL_PATRIM_LIQ"))
fi_painel.write.format("delta").mode("overwrite").saveAsTable(f"{TGT}.gold_fi_painel")

print("GOLD ")

 
