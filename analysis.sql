-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Análisis de negocio
-- MAGIC
-- MAGIC Consultas sobre las tablas silver y gold de `horoz_fraude_pipeline`.
-- MAGIC
-- MAGIC 1. KPIs generales
-- MAGIC 2. Ventas: tendencia, canal, sucursal, día y hora
-- MAGIC 3. Experiencia en comedor y propinas
-- MAGIC 4. Voids y descuentos
-- MAGIC 5. Reglas de negocio violadas
-- MAGIC 6. Riesgo de fraude por empleado
-- MAGIC 7. Tickets que no cuadran
-- MAGIC
-- MAGIC Conectar a cómputo **Serverless** o a un **SQL warehouse**. Cada consulta se puede convertir en
-- MAGIC visualización desde el resultado (botón **+**) o copiarse a un AI/BI Dashboard.

-- COMMAND ----------

-- Todas las tablas van con nombre completo (workspace.horoz.tabla), así cada celda
-- funciona sola aunque no se haya corrido esta.
USE CATALOG workspace;
USE SCHEMA horoz;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. KPIs generales

-- COMMAND ----------

SELECT
  MIN(BusinessDate)                                        AS desde,
  MAX(BusinessDate)                                        AS hasta,
  COUNT(DISTINCT LocationId)                               AS sucursales,
  COUNT(DISTINCT server_id)                                AS meseros,
  COUNT(*)                                                 AS tickets,
  ROUND(SUM(venta_neta), 0)                                AS venta_neta,
  ROUND(AVG(venta_neta), 2)                                AS ticket_promedio,
  ROUND(100 * SUM(propina)     / SUM(venta_neta), 2)       AS pct_propina,
  ROUND(100 * SUM(descuentos)  / SUM(venta), 2)            AS pct_descuento_sobre_venta,
  ROUND(100 * SUM(monto_voids) / SUM(venta), 2)            AS pct_voids_sobre_venta,
  ROUND(100 * AVG(CAST(n_voids > 0 AS INT)), 2)            AS pct_tickets_con_void,
  ROUND(100 * AVG(CAST(n_desc_total > 0 AS INT)), 2)       AS pct_tickets_con_descuento
FROM workspace.horoz.silver_checks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Ventas
-- MAGIC ### 2a. Tendencia mensual

-- COMMAND ----------

SELECT
  DATE_TRUNC('month', BusinessDate)            AS mes,
  SUM(tickets)                                 AS tickets,
  ROUND(SUM(venta_neta), 0)                    AS venta_neta,
  ROUND(SUM(venta_neta) / SUM(tickets), 2)     AS ticket_promedio,
  COUNT(DISTINCT LocationId)                   AS sucursales_activas
FROM workspace.horoz.gold_daily_sales
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2b. Por canal

-- COMMAND ----------

SELECT
  COALESCE(canal, CAST(RevenueCenterId AS STRING))                   AS canal,
  SUM(tickets)                                                       AS tickets,
  ROUND(SUM(venta_neta), 0)                                          AS venta_neta,
  ROUND(100 * SUM(venta_neta) / SUM(SUM(venta_neta)) OVER (), 1)     AS pct_venta,
  ROUND(SUM(venta_neta) / SUM(tickets), 2)                           AS ticket_promedio,
  ROUND(100 * SUM(propinas) / SUM(venta_neta), 2)                    AS pct_propina
FROM workspace.horoz.gold_daily_sales
GROUP BY 1
ORDER BY venta_neta DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2c. Sucursales: mejores y peores por venta diaria promedio
-- MAGIC Se usa venta **por día con operación** para no castigar a sucursales con menos días de datos.

-- COMMAND ----------

WITH s AS (
  SELECT
    LocationId,
    FIRST(sucursal, TRUE)                                   AS sucursal,
    COUNT(DISTINCT BusinessDate)                            AS dias,
    SUM(tickets)                                            AS tickets,
    SUM(venta_neta)                                         AS venta_neta
  FROM workspace.horoz.gold_daily_sales
  GROUP BY LocationId
)
SELECT
  sucursal, LocationId, dias, tickets,
  ROUND(venta_neta, 0)                   AS venta_neta,
  ROUND(venta_neta / dias, 0)            AS venta_diaria_promedio,
  ROUND(venta_neta / tickets, 2)         AS ticket_promedio,
  RANK() OVER (ORDER BY venta_neta / dias DESC) AS ranking
FROM s
ORDER BY venta_diaria_promedio DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2d. Día de la semana

-- COMMAND ----------

WITH dia AS (
  SELECT BusinessDate, SUM(venta_neta) AS venta, SUM(tickets) AS tickets
  FROM workspace.horoz.gold_daily_sales
  GROUP BY BusinessDate
)
SELECT
  DAYOFWEEK(BusinessDate)              AS n,
  DATE_FORMAT(BusinessDate, 'EEEE')    AS dia,
  ROUND(AVG(venta), 0)                 AS venta_promedio_por_dia,
  ROUND(AVG(tickets), 0)               AS tickets_promedio_por_dia,
  ROUND(SUM(venta) / SUM(tickets), 2)  AS ticket_promedio
FROM dia
GROUP BY 1, 2
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2e. Hora del día

-- COMMAND ----------

SELECT
  hora,
  COUNT(*)                                                   AS tickets,
  ROUND(SUM(venta_neta), 0)                                  AS venta_neta,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)           AS pct_tickets,
  ROUND(AVG(venta_neta), 2)                                  AS ticket_promedio
FROM workspace.horoz.silver_checks
WHERE hora IS NOT NULL
GROUP BY hora
ORDER BY hora;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Comedor y propinas

-- COMMAND ----------

SELECT
  COUNT(*)                                                       AS tickets_comedor,
  ROUND(AVG(TrafficCount), 2)                                    AS comensales_por_ticket,
  ROUND(SUM(venta_neta) / SUM(TrafficCount), 2)                  AS venta_por_comensal,
  ROUND(PERCENTILE_APPROX(min_mesa, 0.5), 0)                     AS minutos_mesa_mediana,
  ROUND(100 * SUM(propina) / SUM(venta_neta), 2)                 AS pct_propina
FROM workspace.horoz.silver_checks
WHERE RevenueCenterId = 1
  AND TrafficCount > 0
  AND min_mesa BETWEEN 0 AND 480;

-- COMMAND ----------

-- Propina según tamaño de mesa
SELECT
  CASE WHEN TrafficCount >= 6 THEN '6+' ELSE CAST(CAST(TrafficCount AS INT) AS STRING) END AS comensales,
  COUNT(*)                                          AS tickets,
  ROUND(AVG(venta_neta), 2)                         AS ticket_promedio,
  ROUND(100 * SUM(propina) / SUM(venta_neta), 2)    AS pct_propina,
  ROUND(PERCENTILE_APPROX(min_mesa, 0.5), 0)        AS minutos_mesa_mediana
FROM workspace.horoz.silver_checks
WHERE RevenueCenterId = 1 AND TrafficCount > 0 AND min_mesa BETWEEN 0 AND 480
GROUP BY 1
ORDER BY MIN(TrafficCount);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Voids y descuentos
-- MAGIC ### 4a. Voids por categoría

-- COMMAND ----------

SELECT
  v.VoidCategoryName                                                     AS categoria,
  MAX(v.void_riesgo_alto)                                                AS riesgo_alto,
  COUNT(*)                                                               AS voids,
  ROUND(SUM(v.Amount), 0)                                                AS monto,
  ROUND(100 * SUM(v.Amount) / SUM(SUM(v.Amount)) OVER (), 1)             AS pct_monto,
  ROUND(100 * AVG(CAST(COALESCE(v.ManagerId, 0) = 0 AS INT)), 1)         AS pct_sin_gerente,
  ROUND(100 * AVG(CAST(c.tiene_efectivo AS INT)), 1)                     AS pct_en_ticket_efectivo
FROM workspace.horoz.silver_voids v
LEFT JOIN workspace.horoz.silver_checks c USING (LocationId, BusinessDate, Id)
GROUP BY v.VoidCategoryName
ORDER BY monto DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4b. Descuentos por tipo

-- COMMAND ----------

SELECT
  tipo_descuento,
  COUNT(*)                                                         AS descuentos,
  ROUND(SUM(Amount), 0)                                            AS monto,
  ROUND(100 * SUM(Amount) / SUM(SUM(Amount)) OVER (), 1)           AS pct_monto,
  ROUND(AVG(Amount), 2)                                            AS monto_promedio
FROM workspace.horoz.silver_discounts
GROUP BY tipo_descuento
ORDER BY monto DESC;

-- COMMAND ----------

-- Los 15 descuentos más usados
SELECT DiscountName, tipo_descuento, COUNT(*) AS veces, ROUND(SUM(Amount), 0) AS monto
FROM workspace.horoz.silver_discounts
GROUP BY DiscountName, tipo_descuento
ORDER BY veces DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Reglas de negocio violadas

-- COMMAND ----------

SELECT
  SUM(CAST(n_desc_empleado > 1 AS INT))                              AS mas_de_un_desc_empleado,
  SUM(CAST(n_desc_empleado > 0 AND n_desc_otros > 0 AS INT))         AS desc_empleado_combinado,
  SUM(CAST(n_voids > 0 AND n_desc_total > 0 AS INT))                 AS void_con_descuento,
  SUM(CAST(n_desc_empleado > 1
        OR (n_desc_empleado > 0 AND n_desc_otros > 0)
        OR (n_voids > 0 AND n_desc_total > 0) AS INT))               AS tickets_con_alguna_violacion,
  COUNT(*)                                                           AS tickets
FROM workspace.horoz.silver_checks;

-- COMMAND ----------

-- Sucursales con mayor tasa de violaciones
SELECT
  l.Name                                                             AS sucursal,
  c.LocationId,
  COUNT(*)                                                           AS tickets,
  SUM(CAST(c.n_desc_empleado > 1
        OR (c.n_desc_empleado > 0 AND c.n_desc_otros > 0)
        OR (c.n_voids > 0 AND c.n_desc_total > 0) AS INT))           AS violaciones,
  ROUND(1000 * AVG(CAST(c.n_desc_empleado > 1
        OR (c.n_desc_empleado > 0 AND c.n_desc_otros > 0)
        OR (c.n_voids > 0 AND c.n_desc_total > 0) AS INT)), 2)       AS violaciones_por_mil_tickets
FROM workspace.horoz.silver_checks c
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = c.LocationId
GROUP BY l.Name, c.LocationId
ORDER BY violaciones_por_mil_tickets DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Riesgo de fraude por empleado
-- MAGIC Solo semanas con `muestra_suficiente` (10+ tickets).
-- MAGIC ### 6a. Distribución del risk score

-- COMMAND ----------

SELECT
  COUNT(*)                                                    AS empleado_semanas,
  COUNT(DISTINCT EmployeeId)                                  AS empleados,
  ROUND(PERCENTILE_APPROX(risk_score, 0.50), 2)               AS p50,
  ROUND(PERCENTILE_APPROX(risk_score, 0.90), 2)               AS p90,
  ROUND(PERCENTILE_APPROX(risk_score, 0.99), 2)               AS p99,
  ROUND(MAX(risk_score), 2)                                   AS maximo,
  ROUND(100 * AVG(CAST(risk_score = 0 AS INT)), 1)            AS pct_score_cero
FROM workspace.horoz.gold_employee_weekly_risk
WHERE muestra_suficiente;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6b. Las 20 semanas-empleado más riesgosas

-- COMMAND ----------

SELECT
  l.Name AS sucursal, r.EmployeeId, r.semana, r.tickets,
  ROUND(r.risk_score, 2)                AS risk_score,
  r.rank_en_sucursal,
  r.voids, ROUND(r.tasa_voids, 3)        AS tasa_voids,
  r.voids_riesgo_efectivo, r.voids_sin_gerente,
  ROUND(r.monto_desc_gerente, 0)         AS monto_desc_gerente,
  r.violaciones_regla,
  ROUND(r.z_tasa_voids, 1) AS z_tasa_voids, ROUND(r.z_tasa_monto_voids, 1) AS z_monto_voids,
  ROUND(r.z_voids_riesgo_efectivo, 1) AS z_voids_efectivo, ROUND(r.z_voids_sin_gerente, 1) AS z_sin_gerente,
  ROUND(r.z_monto_desc_gerente, 1) AS z_desc_gerente, ROUND(r.z_tasa_cero_neg, 1) AS z_cero_neg,
  ROUND(r.z_violaciones_regla, 1) AS z_violaciones
FROM workspace.horoz.gold_employee_weekly_risk r
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = r.LocationId
WHERE r.muestra_suficiente
ORDER BY r.risk_score DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6c. ¿Qué señales empujan los scores altos?
-- MAGIC Contribución de cada señal al risk score en el 1% más alto.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6d. Empleados con riesgo persistente
-- MAGIC Una semana mala puede ser casualidad; muchas semanas en el 1% más alto es un patrón.

-- COMMAND ----------



-- COMMAND ----------

WITH umbral AS (
  SELECT PERCENTILE_APPROX(risk_score, 0.99) AS p99
  FROM workspace.horoz.gold_employee_weekly_risk WHERE muestra_suficiente
),
top_riesgo AS (
  SELECT r.* FROM workspace.horoz.gold_employee_weekly_risk r CROSS JOIN umbral u
  WHERE r.muestra_suficiente AND r.risk_score >= u.p99 AND r.risk_score > 0
),
s AS (
  -- Mismo tope de 3.5 que usa el risk_score, para que las contribuciones sumen lo mismo que el score
  SELECT
    SUM(LEAST(GREATEST(z_tasa_voids, 0), 3.5))            AS tasa_voids,
    SUM(LEAST(GREATEST(z_tasa_monto_voids, 0), 3.5))      AS tasa_monto_voids,
    SUM(LEAST(GREATEST(z_voids_riesgo_efectivo, 0), 3.5)) AS voids_riesgo_efectivo,
    SUM(LEAST(GREATEST(z_voids_sin_gerente, 0), 3.5))     AS voids_sin_gerente,
    SUM(LEAST(GREATEST(z_monto_desc_gerente, 0), 3.5))    AS monto_desc_gerente,
    SUM(LEAST(GREATEST(z_tasa_cero_neg, 0), 3.5))         AS tasa_cero_neg,
    SUM(LEAST(GREATEST(z_violaciones_regla, 0), 3.5))     AS violaciones_regla
  FROM top_riesgo
)
SELECT senal, ROUND(100 * valor / SUM(valor) OVER (), 1) AS pct_contribucion
FROM s
UNPIVOT (valor FOR senal IN (tasa_voids, tasa_monto_voids, voids_riesgo_efectivo, voids_sin_gerente,
                             monto_desc_gerente, tasa_cero_neg, violaciones_regla))
ORDER BY pct_contribucion DESC;

-- COMMAND ----------

WITH umbral AS (
  SELECT PERCENTILE_APPROX(risk_score, 0.99) AS p99
  FROM workspace.horoz.gold_employee_weekly_risk WHERE muestra_suficiente
)
SELECT
  l.Name                                                       AS sucursal,
  r.LocationId, r.EmployeeId,
  COUNT(*)                                                     AS semanas_trabajadas,
  SUM(CAST(r.risk_score >= u.p99 AS INT))                      AS semanas_en_top_1pct,
  SUM(CAST(r.rank_en_sucursal = 1 AS INT))                     AS semanas_como_numero_1,
  ROUND(AVG(r.risk_score), 2)                                  AS risk_score_promedio,
  SUM(r.voids)                                                 AS voids_total,
  ROUND(SUM(r.monto_desc_gerente), 0)                          AS monto_desc_gerente_total
FROM workspace.horoz.gold_employee_weekly_risk r
CROSS JOIN umbral u
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = r.LocationId
WHERE r.muestra_suficiente
GROUP BY l.Name, r.LocationId, r.EmployeeId
HAVING SUM(CAST(r.risk_score >= u.p99 AS INT)) >= 2
ORDER BY semanas_en_top_1pct DESC, risk_score_promedio DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6e. Sucursales con más concentración de riesgo

-- COMMAND ----------

WITH umbral AS (
  SELECT PERCENTILE_APPROX(risk_score, 0.99) AS p99
  FROM workspace.horoz.gold_employee_weekly_risk WHERE muestra_suficiente
)
SELECT
  l.Name                                                         AS sucursal,
  r.LocationId,
  COUNT(*)                                                       AS empleado_semanas,
  SUM(CAST(r.risk_score >= u.p99 AS INT))                        AS en_top_1pct,
  ROUND(100 * AVG(CAST(r.risk_score >= u.p99 AS INT)), 2)        AS pct_en_top_1pct
FROM workspace.horoz.gold_employee_weekly_risk r
CROSS JOIN umbral u
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = r.LocationId
WHERE r.muestra_suficiente
GROUP BY l.Name, r.LocationId
HAVING COUNT(*) >= 50
ORDER BY pct_en_top_1pct DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Tickets que no cuadran (`gold_checks_to_review`)

-- COMMAND ----------

SELECT
  motivo,
  COUNT(*)                      AS tickets,
  ROUND(SUM(dif_pago), 2)       AS diferencia_total,
  ROUND(AVG(dif_pago), 2)       AS diferencia_promedio,
  ROUND(MIN(dif_pago), 2)       AS minima,
  ROUND(MAX(dif_pago), 2)       AS maxima
FROM workspace.horoz.gold_checks_to_review
GROUP BY motivo;

-- COMMAND ----------

-- ¿Se concentran en alguna sucursal o mesero?
SELECT
  l.Name AS sucursal, t.LocationId, t.server_id,
  COUNT(*)                     AS tickets,
  ROUND(SUM(t.dif_pago), 2)    AS diferencia_total
FROM workspace.horoz.gold_checks_to_review t
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = t.LocationId
GROUP BY l.Name, t.LocationId, t.server_id
ORDER BY tickets DESC, ABS(SUM(t.dif_pago)) DESC
LIMIT 15;

-- COMMAND ----------

-- Las 10 diferencias más grandes
SELECT
  l.Name AS sucursal, t.BusinessDate, t.Id AS ticket, t.server_id,
  ROUND(t.venta_neta, 2) AS venta_neta, ROUND(t.impuestos, 2) AS impuestos,
  ROUND(t.pagado, 2) AS pagado, ROUND(t.propina, 2) AS propina,
  t.dif_pago, t.motivo
FROM workspace.horoz.gold_checks_to_review t
LEFT JOIN workspace.horoz.bronze_locations l ON l.Id = t.LocationId
ORDER BY ABS(t.dif_pago) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7b. Patrones de los tickets que no cuadran

-- COMMAND ----------

SELECT
  CASE
    WHEN Id = -1                                   THEN '4. Reembolso sin ticket (Id = -1)'
    WHEN pagado = 0                                THEN '1. Venta sin pago registrado'
    WHEN ABS((pagado - propina) - 2 * (venta_neta + impuestos + cargo_servicio)) <= 0.05
                                                   THEN '3. Cobro duplicado'
    WHEN dif_pago < 0                              THEN '2. Pago parcial'
    ELSE '5. Pagó de más (otro)'
  END                              AS patron,
  COUNT(*)                         AS tickets,
  ROUND(SUM(dif_pago), 2)          AS diferencia_total,
  COUNT(DISTINCT LocationId)       AS sucursales
FROM workspace.horoz.gold_checks_to_review
GROUP BY 1
ORDER BY 1;