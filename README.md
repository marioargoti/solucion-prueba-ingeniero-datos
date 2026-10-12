# Acercamiento al rol
# Mario Andres Argoti Caicedo 11/10/2026
# Version 1

## Objetivo

Construye un pipeline que sea capaz de:

a. Cargar todos los archivos .CSV excepto el llamado “validation.csv” (El pipeline
no debe contener todo el conjunto de datos, es decir, los 5 archivos .CSV al mismo
tiempo en memoria en cualquier momento).
b. Almacena los datos de los archivos .CSV, en una base de datos de
tu elección (ejemplo: PostgreSQL, MySQL, etc). El diseño de esta base de
datos dependerá de ti, crea la tabla o tablas que creas necesarias con el
esquema que creas es adecuado, pero ten presente que todos los .CSV
deben ir en la misma base de datos.
c. A medida que los datos son cargados, realiza un seguimiento
de:
• Recuento (Count) de filas cargadas a la base de datos.
• Valor medio, mínimo y máximo del campo “price”.


## Configuracion PostgreSQL - Entorno 

Se requiere Docker Desktop, Python 3.10 o superior y los archivos CSV proporcionados.

1. Iniciar PostgreSQL:

   docker compose up -d
   docker ps
   Resultado: pragma-prueba-postgres state Up

2. Conectarse a la base de dato s

   docker exec -it pragma-prueba-postgres psql -U postgres -d prueba_data
   \q salir del PostgreSQL

3. Crear el entorno, instalar dependencias, ejecucion de pipeline

   python -m venv .venv
   source .venv/bin/activate

   pip install -r requirements.txt

   python pipeline.py --initial
   python pipeline.py --validate


## Ejecución

python pipeline.py --initial

   Resultado inicial: 
   Estadísticas globales de transactions
   -------------------------------------
   row_count: 143
   price_count: 139
   sum_price: 8046.000000
   avg_price: 57.8848920863309353
   min_price: 10.000000
   max_price: 100.000000

   Estadísticas incrementales de pipeline_stats
   ---------------------------------------------
   Filas acumuladas: 143
   Precios conocidos: 139
   Suma acumulada: 8046.000000
   Promedio acumulado: 57.88489208633093525179856115
   Mínimo acumulado: 10.000000
   Máximo acumulado: 100.000000


python pipeline.py --validate

   Estadísticas globales de transactions
   -------------------------------------
   row_count: 151
   price_count: 147
   sum_price: 8380.000000
   avg_price: 57.0068027210884354
   min_price: 10.000000
   max_price: 100.000000

   Estadísticas incrementales de pipeline_stats
   ---------------------------------------------
   Filas acumuladas: 151
   Precios conocidos: 147
   Suma acumulada: 8380.000000
   Promedio acumulado: 57.00680272108843537414965986
   Mínimo acumulado: 10.000000
   Máximo acumulado: 100.000000
   (.venv) marioandresargoticaicedo@Mac solucion_prueba_ingeniero_dato

Los archivos se procesan individualmente y en microbatches, sin cargarlos completos en memoria.

# Tratamiento e ingesta de datos

La tabla `transactions` almacena los registros y su archivo de origen. La tabla `pipeline_stats` mantiene los acumuladores:

- `row_count`: total de filas.
- `price_count`: cantidad de precios validos.
- `sum_price`: suma de precios validos.
- `min_price` y `max_price`: valores extremos.

El promedio se calcula como `sum_price / price_count`. 
Los precios vacios se almacenan como `NULL`

El acumulador se actualiza con cada microbatch, evitando recalcular las estadiisticas sobre toda la tabla. Las consultas SQL independientes permiten verificar los resultados.

# Consultas
Las consultas SQL de comprobación se encuentran en `sql/consultas.sql`.

cat sql/consultas.sql

