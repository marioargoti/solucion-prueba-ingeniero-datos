
import argparse
import csv
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import (
    BigInteger,
    Column,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.exc import SQLAlchemyError


# CONFIGURACIÓN
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost:5432/prueba_data",
)

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
metadata = MetaData()


# DEFINICIÓN DE TABLAS
transactions = Table(
    "transactions",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("timestamp", String(100), nullable=True),
    Column("price", Numeric(20, 6), nullable=True),
    Column("user_id", String(200), nullable=True),
    Column("source_file", String(255), nullable=False),
)

pipeline_stats = Table(
    "pipeline_stats",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=False),
    Column("row_count", BigInteger, nullable=False, default=0),
    Column("price_count", BigInteger, nullable=False, default=0),
    Column("sum_price", Numeric(30, 6), nullable=False, default=0),
    Column("min_price", Numeric(20, 6), nullable=True),
    Column("max_price", Numeric(20, 6), nullable=True),
)


# INICIALIZACIÓN
def initialize():
    """Crea las tablas e inicializa elas estadisticas"""

    metadata.create_all(engine)

    with engine.begin() as connection:
        stats = connection.execute(
            select(pipeline_stats).where(pipeline_stats.c.id == 1)
        ).mappings().first()

        if stats is None:
            connection.execute(
                insert(pipeline_stats).values(
                    id=1,
                    row_count=0,
                    price_count=0,
                    sum_price=Decimal("0"),
                    min_price=None,
                    max_price=None,
                )
            )



# LECTURA Y VALIDACIÓN DE CSV
def parse_rows(path: Path):
    """
    Se convierten los campos vacios en nulos.
    """

    required_columns = {"timestamp", "price", "user_id"}

    with path.open(
        mode="r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError(
                f"El archivo {path.name} está vacío "
                "o no contiene encabezados."
            )

        reader.fieldnames = [
            name.strip() if name else name
            for name in reader.fieldnames
        ]

        missing_columns = required_columns - set(reader.fieldnames)

        if missing_columns:
            raise ValueError(
                f"Faltan columnas en {path.name}: "
                f"{', '.join(sorted(missing_columns))}"
            )

        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise ValueError(
                    f"Hay más valores que columnas en "
                    f"{path.name}, línea {line_number}."
                )

            raw_timestamp = (row.get("timestamp") or "").strip()
            raw_price = (row.get("price") or "").strip()
            raw_user_id = (row.get("user_id") or "").strip()

            if not raw_price:
                price = None
            else:
                try:
                    price = Decimal(raw_price)
                except InvalidOperation:
                    raise ValueError(
                        f"Precio inválido en {path.name}, "
                        f"línea {line_number}: {raw_price!r}"
                    ) from None

                if not price.is_finite():
                    raise ValueError(
                        f"Precio no finito en {path.name}, "
                        f"línea {line_number}: {raw_price!r}"
                    )

            yield {
                "timestamp": raw_timestamp or None,
                "price": price,
                "user_id": raw_user_id or None,
                "source_file": path.name,
            }


# MICROBATCHES
def chunks(iterable, size):
    """Division de los registros en lotes"""

    if size <= 0:
        raise ValueError("BATCH_SIZE Mayor a 0")

    batch = []

    for item in iterable:
        batch.append(item)

        if len(batch) >= size:
            yield batch
            batch = []

    if batch:
        yield batch



# ESTADÍSTICAS INCREMENTALES
def update_incremental_stats(connection, rows):
    """
    Actualiza las estadisticas usando unicamente las filas del lote
    row_count incluye todas las filas
    """

    known_prices = [
        row["price"]
        for row in rows
        if row["price"] is not None
    ]

    batch_count = len(rows)
    batch_price_count = len(known_prices)
    batch_sum = sum(known_prices, Decimal("0"))
    batch_min = min(known_prices) if known_prices else None
    batch_max = max(known_prices) if known_prices else None

    stats = connection.execute(
        select(pipeline_stats)
        .where(pipeline_stats.c.id == 1)
        .with_for_update()
    ).mappings().first()

    if stats is None:
        raise RuntimeError(
            "No existe el registro inicial de pipeline_stats."
        )

    previous_min = stats["min_price"]
    previous_max = stats["max_price"]

    if batch_min is not None:
        new_min = (
            batch_min
            if previous_min is None
            else min(previous_min, batch_min)
        )
        new_max = (
            batch_max
            if previous_max is None
            else max(previous_max, batch_max)
        )
    else:
        new_min = previous_min
        new_max = previous_max

    connection.execute(
        update(pipeline_stats)
        .where(pipeline_stats.c.id == 1)
        .values(
            row_count=stats["row_count"] + batch_count,
            price_count=stats["price_count"] + batch_price_count,
            sum_price=stats["sum_price"] + batch_sum,
            min_price=new_min,
            max_price=new_max,
        )
    )



# CARGA DE ARCHIVOS
def load_file(path: Path):
    """Carga un CSV y actualiza estadisticas en cada microbatch"""

    print(
        f"Procesando {path.name} en microbatches "
        f"de {BATCH_SIZE} filas..."
    )

    processed = 0

    for batch in chunks(parse_rows(path), BATCH_SIZE):
        # insercion y estadisticas se confirman juntas.
        with engine.begin() as connection:
            connection.execute(insert(transactions), batch)
            update_incremental_stats(connection, batch)

        processed += len(batch)

        print(
            f"  Lote confirmado: {len(batch)} filas. "
            f"Total del archivo: {processed}"
        )

    print(
        f"Archivo {path.name} procesado. "
        f"Filas cargadas: {processed}"
    )

# CONSULTA GLOBAL INDEPENDIENTE
def query_database():
    """Se calcula estadssticas directamente desde transactions."""

    with engine.connect() as connection:
        result = connection.execute(
            select(
                func.count().label("row_count"),
                func.count(transactions.c.price).label("price_count"),
                func.sum(transactions.c.price).label("sum_price"),
                func.avg(transactions.c.price).label("avg_price"),
                func.min(transactions.c.price).label("min_price"),
                func.max(transactions.c.price).label("max_price"),
            )
        ).mappings().one()

        print("\nEstadisticas globales de transactions")
        print("-------------------------------------")
        for key, value in result.items():
            print(f"{key}: {value}")

        stats = connection.execute(
            select(pipeline_stats).where(pipeline_stats.c.id == 1)
        ).mappings().first()

        if stats:
            avg_price = (
                stats["sum_price"] / stats["price_count"]
                if stats["price_count"] > 0
                else None
            )

            print("\nEstadisticas incrementales de pipeline_stats")
            print("---------------------------------------------")
            print(f"Filas acumuladas: {stats['row_count']}")
            print(f"Precios conocidos: {stats['price_count']}")
            print(f"Suma acumulada: {stats['sum_price']}")
            print(f"Promedio acumulado: {avg_price}")
            print(f"Minimo acumulado: {stats['min_price']}")
            print(f"Maximo acumulado: {stats['max_price']}")


# SELECCIÓN DE ARCHIVOS

def list_initial_files():
    """Obtiene los CSV iniciales excluyendo validation.csv."""

    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"No existe el directorio de datos: {DATA_DIR}"
        )

    files = sorted(
        path
        for path in DATA_DIR.glob("*.csv")
        if path.name.lower() != "validation.csv"
    )

    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos CSV en {DATA_DIR}"
        )

    return files


# EJECUCIÓN PRINCIPAL

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline de carga y validacion de datos."
    )

    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument(
        "--initial",
        action="store_true",
        help="Carga los archivos CSV iniciales.",
    )
    modes.add_argument(
        "--validate",
        action="store_true",
        help="Carga data/validation.csv.",
    )

    args = parser.parse_args()

    try:
        initialize()

        if args.initial:
            for path in list_initial_files():
                load_file(path)

        elif args.validate:
            validation_file = DATA_DIR / "validation.csv"

            if not validation_file.exists():
                raise FileNotFoundError(
                    f"No existe el archivo: {validation_file}"
                )

            load_file(validation_file)

        query_database()

    except (
        ValueError,
        FileNotFoundError,
        SQLAlchemyError,
        OSError,
        RuntimeError,
    ) as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1) from exc

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
