
import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window

BASE = "/Volumes/workspace/default/applebees/Applebees_Challenge"
KEY = ["LocationId", "BusinessDate", "Id"]          # llave única de un ticket
EMP_REGEX = "^(EMP |MIT MEAL)"                       # descuentos de empleado
VOIDS_RIESGO = ["NOT RECVD", "OTHER", "WRONG CHK"]  # justificaciones no verificables


def suma(arr, campo):
    """Suma un campo dentro de una lista anidada, tolerando listas y valores nulos."""
    return F.aggregate(F.coalesce(F.col(arr), F.array()), F.lit(0.0),
                       lambda acc, x: acc + F.coalesce(x[campo].cast("double"), F.lit(0.0)))


def semana(df):
    """Agrega la semana del BusinessDate (base para agrupar en periodos fiscales)."""
    return df.withColumn("semana", F.date_trunc("week", "BusinessDate").cast("date"))


# =============================================================================
# BRONZE
# =============================================================================

@dlt.table(name="bronze_checks",
           comment="Tickets crudos en JSON; ingesta incremental con Auto Loader (solo procesa archivos nuevos)")
def bronze_checks():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "ServiceCharge DOUBLE, TrafficCount DOUBLE")
            .option("cloudFiles.partitionColumns", "brand,dt")
            .option("cloudFiles.maxFilesPerTrigger", "2")   # lotes chicos para no quedarse sin memoria
            .load(f"{BASE}/facts/checks")
            .select("*",
                    F.col("_metadata.file_path").alias("source_file"),
                    F.current_timestamp().alias("ingested_at")))


def dim(nombre, ruta):
    @dlt.table(name=f"bronze_{nombre}", comment=f"Catálogo de {nombre}")
    def _t():
        return spark.read.option("multiLine", "true").json(f"{BASE}/dimensions/{ruta}")


dim("locations", "locations/locations.json")
dim("revenue_centers", "revenue_centers/revenueCenters.json")
dim("items", "items/items.json")
dim("employees", "employees/employees.json")


# =============================================================================
# SILVER
# =============================================================================

@dlt.view(comment="Tickets sin duplicados; llave única LocationId + BusinessDate + Id")
@dlt.expect_or_drop("llave_completa", "Id IS NOT NULL AND LocationId IS NOT NULL AND BusinessDate IS NOT NULL")
def checks_dedup():
    return dlt.read("bronze_checks").dropDuplicates(KEY)


@dlt.table(name="silver_checks", cluster_by=["LocationId", "BusinessDate"],
           comment="Un renglón por ticket: métricas de dinero y tiempo, banderas de calidad y reglas de negocio")
# Calidad de datos (solo avisan, no borran: la evidencia de fraude se conserva)
@dlt.expect("horas_validas", "close_ts >= open_ts")
@dlt.expect("pago_cuadra", "abs(dif_pago) <= 0.05")
@dlt.expect("comensales_en_comedor", "RevenueCenterId <> 1 OR TrafficCount > 0")
# Reglas de negocio confirmadas por los dueños del dataset
@dlt.expect("un_desc_empleado_por_ticket", "n_desc_empleado <= 1")
@dlt.expect("desc_empleado_no_combinado", "n_desc_empleado = 0 OR n_desc_otros = 0")
@dlt.expect("void_no_combinado_con_desc", "n_voids = 0 OR n_desc_total = 0")
def silver_checks():
    return (dlt.read("checks_dedup")
        # Tipos
        .withColumn("BusinessDate", F.to_date("BusinessDate"))
        .withColumn("open_ts", F.to_timestamp("OpenTime"))
        .withColumn("close_ts", F.to_timestamp("CloseTime"))
        # Dinero
        .withColumn("venta", suma("ItemsSold", "SoldPrice"))
        .withColumn("descuentos", suma("Discounts", "Amount"))
        .withColumn("impuestos", suma("Tax", "Amount"))
        .withColumn("cargo_servicio", F.coalesce("ServiceCharge", F.lit(0.0)))
        .withColumn("pagado", suma("Payments", "Amount"))                 # incluye la propina
        .withColumn("propina", -suma("Payments", "Gratuity"))             # Gratuity viene en negativo
        .withColumn("monto_voids", suma("Voids", "Amount"))
        .withColumn("venta_neta", F.col("venta") - F.col("descuentos"))
        # Cuadre: pagado - propina = venta neta + impuestos + cargo de servicio
        .withColumn("dif_pago", F.round(F.col("pagado") - F.col("propina")
                    - (F.col("venta_neta") + F.col("impuestos") + F.col("cargo_servicio")), 2))
        .withColumn("pago_cuadra", F.abs("dif_pago") <= 0.05)
        # Banderas
        .withColumn("is_void_or_comp", F.col("venta") <= 0)
        .withColumn("tiene_efectivo",
                    F.exists(F.coalesce("Payments", F.array()), lambda p: p.PayCatName == "CASH"))
        # Mesero del ticket (validado: un solo empleado por ticket)
        .withColumn("server_id", F.coalesce(F.try_element_at("Payments", F.lit(1))["EmployeeId"],
                                            F.try_element_at("ItemsSold", F.lit(1))["EmployeeId"]))
        # Reglas de descuentos y voids
        .withColumn("_desc", F.coalesce("Discounts", F.array()))
        .withColumn("_voids", F.coalesce("Voids", F.array()))
        .withColumn("n_desc_total", F.size("_desc"))
        .withColumn("n_desc_empleado",
                    F.size(F.filter("_desc", lambda d: F.upper(F.trim(d.DiscountName)).rlike(EMP_REGEX))))
        .withColumn("n_desc_otros", F.col("n_desc_total") - F.col("n_desc_empleado"))
        .withColumn("monto_desc_manager",
                    F.aggregate(F.filter("_desc", lambda d: F.upper(F.trim(d.DiscountName)) == "MANAGER"),
                                F.lit(0.0), lambda a, d: a + F.coalesce(d.Amount, F.lit(0.0))))
        .withColumn("n_voids", F.size("_voids"))
        # Void en ticket pagado en efectivo (no implica orden temporal: los voids no traen hora)
        .withColumn("void_en_efectivo", (F.col("n_voids") > 0) & F.col("tiene_efectivo"))
        .withColumn("void_sin_gerente",
                    F.exists("_voids", lambda v: F.coalesce(v.ManagerId, F.lit(0)) == 0))
        # Tiempo
        .withColumn("hora", F.hour("open_ts"))
        .withColumn("min_mesa", (F.unix_timestamp("close_ts") - F.unix_timestamp("open_ts")) / 60)
        .drop("ItemsSold", "Payments", "Tax", "Discounts", "Voids", "_desc", "_voids"))


@dlt.table(name="dq_gate_silver",
           comment="Compuerta de calidad: resume silver_checks en una fila; si una regla falla, la actualización se detiene")
# Hoy no cuadran 78 de ~2.14 M tickets (0.004%). Si de pronto no cuadra el 1% o más,
# no es fraude: cambió la estructura de los pagos (p. ej. Gratuity dejó de venir en
# negativo) y venta neta, propinas, tasas y risk score saldrían mal.
@dlt.expect_or_fail("hay_tickets", "tickets > 0")
@dlt.expect_or_fail("llave_unica", "tickets = llaves_distintas")
@dlt.expect_or_fail("estructura_pagos_valida", "pct_no_cuadra <= 1.0")
def dq_gate_silver():
    return (dlt.read("silver_checks")
            .agg(F.count("*").alias("tickets"),
                 F.countDistinct(*KEY).alias("llaves_distintas"),
                 (100 * F.avg((~F.col("pago_cuadra")).cast("double"))).alias("pct_no_cuadra"),
                 F.max("BusinessDate").alias("ultima_fecha"))
            .withColumn("evaluado_en", F.current_timestamp()))


@dlt.table(name="silver_items", cluster_by=["LocationId", "BusinessDate"],
           comment="Un renglón por producto vendido; es_modificador separa guarniciones y términos")
@dlt.expect("precio_no_negativo", "SoldPrice >= 0")
def silver_items():
    return (dlt.read("checks_dedup")
            .select(*KEY, F.explode("ItemsSold").alias("i"))
            .select(*KEY, "i.*")
            .drop("Discounts")
            .withColumn("BusinessDate", F.to_date("BusinessDate"))
            .withColumn("es_modificador", F.col("ParentSequence") != 0))


@dlt.table(name="silver_voids",
           comment="Productos cancelados con categoría normalizada, empleado y gerente que autorizó")
def silver_voids():
    return (dlt.read("checks_dedup")
            .select(*KEY, F.explode("Voids").alias("v"))
            .select(*KEY, "v.*")
            .withColumn("BusinessDate", F.to_date("BusinessDate"))
            .withColumn("VoidCategoryName", F.upper(F.trim("VoidCategoryName")))
            .withColumn("VoidCategoryName",
                        F.when(F.col("VoidCategoryName") == "SERV ERR", "SERVER ERR")
                         .when(F.col("VoidCategoryName") == "NOT RECEVD", "NOT RECVD")
                         .otherwise(F.col("VoidCategoryName")))
            .withColumn("void_riesgo_alto", F.col("VoidCategoryName").isin(VOIDS_RIESGO)))


@dlt.table(name="silver_discounts",
           comment="Descuentos a nivel ticket con nombre normalizado y tipo (empleado, gerente, lealtad, promoción)")
def silver_discounts():
    return (dlt.read("checks_dedup")
            .select(*KEY, F.explode("Discounts").alias("d"))
            .select(*KEY, "d.*")
            .withColumn("BusinessDate", F.to_date("BusinessDate"))
            .withColumn("DiscountName", F.upper(F.trim("DiscountName")))
            .withColumn("tipo_descuento",
                        F.when(F.col("DiscountName").rlike(EMP_REGEX), "empleado")
                         .when(F.col("DiscountName").isin("MANAGER", "COMP BALANCE: 0.00"), "gerente")
                         .when(F.col("DiscountName").rlike("PUNCHH|CLUB APPLEBEE|LOYALTY|GIFT|MDG|BONUS"), "lealtad")
                         .otherwise("promocion")))


# =============================================================================
# GOLD
# =============================================================================

@dlt.table(name="gold_daily_sales",
           comment="Venta diaria por sucursal y canal para análisis de tendencias")
def gold_daily_sales():
    loc = dlt.read("bronze_locations").select(F.col("Id").alias("LocationId"), F.col("Name").alias("sucursal"))
    rc = dlt.read("bronze_revenue_centers").select(F.col("Id").alias("RevenueCenterId"), F.col("Name").alias("canal"))
    return (dlt.read("silver_checks")
            .groupBy("BusinessDate", "LocationId", "RevenueCenterId")
            .agg(F.count("*").alias("tickets"),
                 F.round(F.sum("venta_neta"), 2).alias("venta_neta"),
                 # Comensales solo cuentan en comedor: en para llevar y delivery no hay mesa
                 F.sum(F.when(F.col("RevenueCenterId") == 1, F.col("TrafficCount"))).alias("comensales_comedor"),
                 F.round(F.sum("propina"), 2).alias("propinas"))
            .join(loc, "LocationId", "left")
            .join(rc, "RevenueCenterId", "left"))


@dlt.table(name="gold_employee_activity", cluster_by=["LocationId", "EmployeeId"],
           comment="Resumen de actividad por empleado y sucursal en todo el periodo")
def gold_employee_activity():
    k = ["LocationId", "EmployeeId"]
    chk = (dlt.read("silver_checks").withColumnRenamed("server_id", "EmployeeId")
           .groupBy(*k).agg(
               F.count("*").alias("tickets"),
               F.sum("venta_neta").alias("venta_neta"),
               F.sum("propina").alias("propinas"),
               F.sum(F.col("is_void_or_comp").cast("int")).alias("tickets_cero_neg"),
               F.sum((~F.col("pago_cuadra")).cast("int")).alias("tickets_no_cuadran")))
    v = dlt.read("silver_voids").groupBy(*k).agg(
        F.count("*").alias("voids"), F.sum("Amount").alias("monto_voids"))
    d = dlt.read("silver_discounts").groupBy(*k).agg(
        F.count("*").alias("descuentos"), F.sum("Amount").alias("monto_descuentos"))
    return (chk.join(v, k, "full").join(d, k, "full").fillna(0)
            .withColumn("tasa_voids", F.try_divide(F.col("voids"), F.col("tickets")))
            .withColumn("tasa_cero_neg", F.try_divide(F.col("tickets_cero_neg"), F.col("tickets")))
            .withColumn("tasa_descuento", F.try_divide(F.col("monto_descuentos"), F.col("venta_neta")))
            .withColumn("tasa_propina", F.try_divide(F.col("propinas"), F.col("venta_neta"))))


@dlt.table(name="gold_checks_to_review",
           comment="Tickets que no cuadran entre venta y pago, candidatos a revisión")
def gold_checks_to_review():
    return (dlt.read("silver_checks")
            .filter(~F.col("pago_cuadra"))
            .withColumn("motivo", F.when(F.col("dif_pago") < 0, "pagó menos de lo vendido")
                                   .otherwise("pagó más de lo vendido")))


@dlt.table(name="gold_employee_weekly_risk", cluster_by=["LocationId", "semana"],
           comment="Score de riesgo semanal por empleado, comparado contra sus compañeros de la misma sucursal")
def gold_employee_weekly_risk():
    k = ["LocationId", "EmployeeId", "semana"]
    chk = semana(dlt.read("silver_checks")).withColumnRenamed("server_id", "EmployeeId")

    base = chk.groupBy(*k).agg(
        F.count("*").alias("tickets"),
        F.sum("venta_neta").alias("venta_neta"),
        F.sum(F.col("is_void_or_comp").cast("int")).alias("tickets_cero_neg"),
        F.sum(((F.col("n_desc_empleado") > 1)
               | ((F.col("n_desc_empleado") > 0) & (F.col("n_desc_otros") > 0))
               | ((F.col("n_voids") > 0) & (F.col("n_desc_total") > 0))).cast("int")).alias("violaciones_regla"))

    v = (semana(dlt.read("silver_voids")
                .join(dlt.read("silver_checks").select(*KEY, "tiene_efectivo"), KEY, "left"))
         .groupBy(*k).agg(
             F.count("*").alias("voids"),
             F.sum("Amount").alias("monto_voids"),
             F.sum((F.col("void_riesgo_alto") & F.coalesce("tiene_efectivo", F.lit(False))).cast("int"))
              .alias("voids_riesgo_efectivo"),
             F.sum((F.coalesce("ManagerId", F.lit(0)) == 0).cast("int")).alias("voids_sin_gerente")))

    d = (semana(dlt.read("silver_discounts"))
         .groupBy(*k).agg(
             F.sum(F.when(F.col("tipo_descuento") == "gerente", F.col("Amount")).otherwise(0.0))
              .alias("monto_desc_gerente")))

    df = (base.join(v, k, "full").join(d, k, "full").fillna(0)
          .withColumn("tasa_voids", F.try_divide(F.col("voids"), F.col("tickets")))
          .withColumn("tasa_monto_voids", F.try_divide(F.col("monto_voids"), F.col("venta_neta")))
          .withColumn("tasa_cero_neg", F.try_divide(F.col("tickets_cero_neg"), F.col("tickets"))))

    # z-score ROBUSTO de cada señal contra los compañeros de la misma sucursal y semana:
    #   z = (x - mediana) / (1.4826 * MAD)
    # Si el MAD es 0 (casi todos en 0), se usa la desviación absoluta media como respaldo.
    # El score suma solo las desviaciones hacia arriba.
    feats = ["tasa_voids", "tasa_monto_voids", "voids_riesgo_efectivo", "voids_sin_gerente",
             "monto_desc_gerente", "tasa_cero_neg", "violaciones_regla"]
    g = ["LocationId", "semana"]
    w = Window.partitionBy(*g)

    med = df.groupBy(*g).agg(*[F.percentile_approx(f, 0.5).alias(f"med_{f}") for f in feats])
    df = df.join(med, g, "left")
    for f in feats:
        df = df.withColumn(f"ad_{f}", F.abs(F.col(f) - F.col(f"med_{f}")))
    esc = df.groupBy(*g).agg(*[c for f in feats for c in (
        F.percentile_approx(f"ad_{f}", 0.5).alias(f"mad_{f}"),
        F.avg(f"ad_{f}").alias(f"meanad_{f}"))])
    df = df.join(esc, g, "left")
    for f in feats:
        escala = F.coalesce(F.nullif(1.4826 * F.col(f"mad_{f}"), F.lit(0.0)),
                            F.nullif(1.2533 * F.col(f"meanad_{f}"), F.lit(0.0)))
        df = df.withColumn(f"z_{f}", F.coalesce(
            F.try_divide(F.col(f) - F.col(f"med_{f}"), escala), F.lit(0.0)))
    df = (df.withColumn("risk_score", sum(F.least(F.greatest(F.col(f"z_{f}"), F.lit(0.0)), F.lit(3.5)) for f in feats))
            .drop(*[c for f in feats for c in (f"med_{f}", f"ad_{f}", f"mad_{f}", f"meanad_{f}")]))

    return (df.withColumn("muestra_suficiente", F.col("tickets") >= 10)
              .withColumn("rank_en_sucursal", F.rank().over(w.orderBy(F.desc("risk_score")))))