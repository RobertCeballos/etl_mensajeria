import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables : list,conection: Engine)-> pd.DataFrame:
    a = []
    for i in tables:
        aux = pd.read_sql_table(i, conection)
        a.append(aux)
    return a


def extract_cliente(conection: Engine):
    dim_cliente = pd.read_sql_query('select cliente_id, nit_cliente, nombre, email, direccion, telefono from cliente', conection)
    return dim_cliente



def extract_mensajero(conection: Engine):
    query = """
    SELECT DISTINCT ON (au.id)
    au.id,
    au.username,
    au.email,
    mt.nombre AS tipo_transporte,
    au.is_active
    FROM auth_user au
    JOIN mensajeria_servicio ms ON ms.mensajero_id = au.id
    JOIN mensajeria_tipovehiculo mt ON ms.tipo_vehiculo_id = mt.id
    ORDER BY au.id, ms.id
    """
    dim_mensajero = pd.read_sql_query(query, con=conection)
    return dim_mensajero


def extract_estado_servicio(conection: Engine):
    dim_estado_servicio = pd.read_sql_table('mensajeria_estado', conection)
    return dim_estado_servicio

def extract_servicio(conection: Engine):
    dim_servicio= pd.read_sql_query('select id, cliente_id, fecha_solicitud, hora_solicitud, mensajero_id, prioridad, ciudad_origen_id from mensajeria_servicio', conection)
    return dim_servicio

def extract_prioridad(conection: Engine):
    dim_prioridad= pd.read_sql_query('SELECT DISTINCT prioridad FROM mensajeria_servicio', conection)
    dim_prioridad["prioridad_id"] = range(1, len(dim_prioridad) + 1)
    return dim_prioridad


def extract_ciudad(conection: Engine):
    dim_ciudad= pd.read_sql_table('ciudad', conection)
    return dim_ciudad

def extract_novedad(conection: Engine):
    dim_novedad= pd.read_sql_table('mensajeria_tiponovedad', conection)
    return dim_novedad

def extract_hecho_solicitud_servicios(conection: Engine):
    dim_servicio = pd.read_sql_table('dim_servicio', conection)
    dim_hora = pd.read_sql_table('dim_hora', conection)
    dim_fecha = pd.read_sql_table('dim_fecha', conection)
    dim_cliente = pd.read_sql_table('dim_cliente', conection)
    dim_ciudad = pd.read_sql_table('dim_ciudad', conection)
    dim_prioridad = pd.read_sql_table('dim_prioridad', conection)
    
    return [dim_servicio, dim_hora, dim_fecha, dim_cliente, dim_ciudad, dim_prioridad]

def extract_hecho_ejecucion_servicios(conn_rel: Engine, conn_dw: Engine):
# Extraer registros válidos (no de prueba) desde la base relacional
    raw_query = """
        SELECT
            me.servicio_id,
            ms.mensajero_id,
            me.estado_id,
            me.fecha,
            me.hora,
            mn.tipo_novedad_id AS novedad_id
        FROM mensajeria_estadosservicio me
        LEFT JOIN mensajeria_servicio ms ON me.servicio_id = ms.id
        LEFT JOIN (
            SELECT DISTINCT ON (servicio_id) servicio_id, tipo_novedad_id
            FROM mensajeria_novedadesservicio
            ORDER BY servicio_id, tipo_novedad_id DESC  -- o cualquier otro criterio
        ) mn ON me.servicio_id = mn.servicio_id;
    """
    raw_df = pd.read_sql(raw_query, conn_rel)

    # Extraer dimensiones necesarias desde la bodega
    dimensiones = {}

    dimensiones['dim_estado_servicio'] = pd.read_sql("SELECT id FROM dim_estado_servicio", conn_dw)
    dimensiones['dim_fecha'] = pd.read_sql("SELECT fecha_id, fecha FROM dim_fecha", conn_dw)
    dimensiones['dim_hora'] = pd.read_sql("SELECT hora_id, hora, minuto FROM dim_hora", conn_dw)
    dimensiones['dim_servicio'] = pd.read_sql("SELECT id FROM dim_servicio", conn_dw)
    dimensiones['dim_mensajero'] = pd.read_sql("SELECT id FROM dim_mensajero", conn_dw)
    dimensiones['dim_novedad'] = pd.read_sql("SELECT id FROM dim_novedad", conn_dw)
    
    return raw_df, dimensiones




#%%
