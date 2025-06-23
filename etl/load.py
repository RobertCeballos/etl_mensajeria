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
    hecho_ejecucion_servicios.to_sql('hecho_ejecucion_servicios', etl_conn, if_exists='append', index=False)

def load(table: DataFrame, etl_conn: Engine, tname, replace: bool = False):
    """

    :param table: table to load into the database
    :param etl_conn: sqlalchemy engine to connect to the database
    :param tname: table name to load into the database
    :param replace:  when true it deletes existing table data(rows)
    :return: void it just load the table to the database
    """
    # statement = insert(f'{table})
    # with etl_conn.connect() as conn:
    #     conn.execute(statement)
    if replace :
        with etl_conn.connect() as conn:
            conn.execute(text(f'Delete from {tname}'))
            conn.close()
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
    else :
        table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
