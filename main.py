import pandas as pd
import datetime
from datetime import date
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import yaml
from etl import extract, transform, load, utils_etl
import psycopg2

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)


with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_co = config['CO_SA']
    config_etl = config['ETL_PRO']

# Construct the database URL
url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
          f"{config_co['port']}/{config_co['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
co_sa = create_engine(url_co)
etl_conn = create_engine(url_etl)

inspector = inspect(etl_conn)
tnames = inspector.get_table_names()

if not tnames:
    conn = psycopg2.connect(dbname=config_etl['dbname'], user=config_etl['user'], password=config_etl['password'],
                            host=config_etl['host'], port=config_etl['port'])
    cur = conn.cursor()
    with open('sqlscripts.yml', 'r') as f:
        sql = yaml.safe_load(f)
        for key, val in sql.items():
            cur.execute(val)
            conn.commit()

#if utils_etl.new_data(etl_conn):

if config['LOAD_DIMENSIONS']:
    dim_cliente = extract.extract_cliente(co_sa)
    dim_ciudad = extract.extract_ciudad(co_sa)
    dim_mensajero = extract.extract_mensajero(co_sa)
    dim_estado_servicio = extract.extract_estado_servicio(co_sa)
    dim_servicio = extract.extract_servicio(co_sa)
    dim_prioridad = extract.extract_prioridad(co_sa)
    dim_novedad = extract.extract_novedad(co_sa)

    # transform
    dim_cliente = transform.transform_cliente(dim_cliente)
    dim_ciudad = transform.transform_ciudad(dim_ciudad)
    dim_mensajero = transform.transform_mensajero(dim_mensajero)
    dim_estado_servicio = transform.transform_estado_servicio(dim_estado_servicio)
    dim_fecha = transform.transform_fecha()
    #dim_hora = transform.transform_hora()
    dim_novedad = transform.transform_novedad(dim_novedad)
    dim_prioridad = transform.transform_prioridad(dim_prioridad)
    dim_servicio = transform.transform_servicio(dim_servicio)



    #load.load(dim_cliente, etl_conn, 'dim_cliente', True)
    #load.load(dim_fecha, etl_conn, 'dim_fecha', True)
    #load.load(dim_servicio, etl_conn, 'dim_servicio', True)
    #load.load(dim_ciudad, etl_conn, 'dim_ciudad', True)
    #load.load(dim_mensajero, etl_conn, 'dim_mensajero', True)
    #load.load(dim_estado_servicio, etl_conn, 'dim_estado_servicio', True)
    #load.load(dim_prioridad, etl_conn, 'dim_prioridad', True)
    #load.load(dim_novedad, etl_conn, 'dim_novedad', True)
    #load.load(dim_hora,etl_conn,'dim_hora',True)


#hecho solicitud servicios
hecho_solicitud_servicios = extract.extract_hecho_solicitud_servicios(etl_conn)
hecho_solicitud_servicios = transform.transform_hecho_solicitud_servicios(hecho_solicitud_servicios)
load.load_hecho_solicitud_servicios(hecho_solicitud_servicios, etl_conn)
print('Done servicios fact')

# Hecho ejecucion servicios
hecho_ejecucion_servicios = extract.extract_hecho_ejecucion_servicios(co_sa,etl_conn)
hecho_ejecucion_servicios = transform.transform_hecho_ejecucion_servicios(hecho_ejecucion_servicios)
load.load_hecho_ejecucion_servicios(hecho_ejecucion_servicios, etl_conn)
print('Done ejecucion servicios fact')
# medicamentos que mas se recetan juntos
#masrecetados = masrecetados.astype('string')
#load.load(masrecetados,etl_conn, 'mas_recetados', False)

print('success all facts loaded')
#else:
#    print('done not new data')

#%%
