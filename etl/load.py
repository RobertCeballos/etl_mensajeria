import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy import text
import yaml
from sqlalchemy.dialects.postgresql import insert


def load_data_cliente(dim_cliente: DataFrame, etl_conn):
    dim_cliente.to_sql('dim_cliente', etl_conn, if_exists='append', index_label='key_dim_cliente')


def load_data_ciudad(dim_ciudad: DataFrame, etl_conn: Engine):
    dim_ciudad.to_sql('dim_ciudad', etl_conn, if_exists='append', index_label='key_dim_ciudad')


def load_data_mensajero(dim_mensajero: DataFrame, etl_conn: Engine):
    dim_mensajero.to_sql('dim_mensajero', con=etl_conn, index_label='key_dim_mensajero', if_exists='append')


def load_data_estado_servicio(dim_estado_servicio: DataFrame, etl_conn: Engine):
    dim_estado_servicio.to_sql('dim_estado_servicio', etl_conn, if_exists='append', index_label='key_dim_estado_servicio')


def load_data_fecha(dim_fecha: DataFrame, etl_conn: Engine):
    dim_fecha.to_sql('dim_fecha', etl_conn, if_exists='append', index_label='key_dim_fecha')


def load_data_hora(dim_hora: DataFrame, etl_conn: Engine):
    dim_hora.to_sql('dim_hora', etl_conn, if_exists='append', index_label='key_dim_hora')

def load_data_novedad(dim_novedad: DataFrame, etl_conn: Engine):
    dim_novedad.to_sql('dim_novedad', etl_conn, if_exists='append', index_label='key_dim_novedad')

def load_data_prioridad(dim_prioridad: DataFrame, etl_conn: Engine):
    dim_prioridad.to_sql('dim_prioridad', etl_conn, if_exists='append', index_label='key_dim_prioridad')

def load_data_servicio(dim_servicio: DataFrame, etl_conn: Engine):
    dim_servicio.to_sql('dim_servicio', etl_conn, if_exists='append', index_label='key_dim_servicio')

def load_hecho_solicitud_servicios(hecho_solicitud_servicios: DataFrame, etl_conn: Engine):
    hecho_solicitud_servicios.to_sql('hecho_solicitud_servicios', etl_conn, if_exists='append', index=False)


def load_hecho_ejecucion_servicios(hecho_ejecucion_servicios: DataFrame, etl_conn: Engine):
    from sqlalchemy import text
    import pandas as pd
    import math

    with etl_conn.connect() as connection:
        trans = connection.begin()
        try:
            insert_sql = text("""
                INSERT INTO hecho_ejecucion_servicios (
                    servicio_id, estado_servicio_id, fecha_estado_id,
                    hora_estado_id, mensajero_id, novedad_id, tiempo_fase_min
                )
                VALUES (
                    :servicio_id, :estado_servicio_id, :fecha_estado_id,
                    :hora_estado_id, :mensajero_id, :novedad_id, :tiempo_fase_min
                )
            """)

            df = hecho_ejecucion_servicios.where(pd.notnull(hecho_ejecucion_servicios), None)

            for _, row in df.iterrows():
                connection.execute(insert_sql, {
                    'servicio_id': int(row['servicio_id']) if not pd.isna(row['servicio_id']) else None,
                    'estado_servicio_id': int(row['estado_servicio_id']) if not pd.isna(row['estado_servicio_id']) else None,
                    'fecha_estado_id': int(row['fecha_estado_id']) if not pd.isna(row['fecha_estado_id']) else None,
                    'hora_estado_id': int(row['hora_estado_id']) if not pd.isna(row['hora_estado_id']) else None,
                    'mensajero_id': int(row['mensajero_id']) if not pd.isna(row['mensajero_id']) else None,
                    'novedad_id': int(row['novedad_id']) if not pd.isna(row['novedad_id']) else None,
                    'tiempo_fase_min': float(row['tiempo_fase_min']) if not pd.isna(row['tiempo_fase_min']) else None
                })

            trans.commit()
            print("Carga a hecho_ejecucion_servicios completada.")
        except Exception as e:
            trans.rollback()
            print("Error durante la carga:", e)




def load(table: DataFrame, etl_conn: Engine, tname, replace: bool = False):
    if replace :
        with etl_conn.connect() as conn:
            conn.execute(text(f'Delete from {tname}'))
            conn.close()
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
    else :
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
