import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables : list,conection: Engine)-> pd.DataFrame:
    """
    :param conection: the conectionnection to the database
    :param tables: the tables to extract
    :return: a list of tables in df format
    """
    a = []
    for i in tables:
        aux = pd.read_sql_table(i, conection)
        a.append(aux)
    return a


def extract_cliente(conection: Engine):
    """
    Extract data from database where the conectionexion established
    :param conection:
    :return:
    """
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
            servicio_id,
            estado_id,
            fecha,
            hora
        FROM mensajeria_estadosservicio;
    """
    raw_df = pd.read_sql(raw_query, conn_rel)

    # Extraer dimensiones necesarias desde la bodega
    dimensiones = {}

    dimensiones['dim_estado_servicio'] = pd.read_sql("SELECT id FROM dim_estado_servicio", conn_dw)
    dimensiones['dim_fecha'] = pd.read_sql("SELECT fecha_id, fecha FROM dim_fecha", conn_dw)
    dimensiones['dim_hora'] = pd.read_sql("SELECT hora_id, hora, minuto FROM dim_hora", conn_dw)
    dimensiones['dim_servicio'] = pd.read_sql("SELECT id FROM dim_servicio", conn_dw)

    return raw_df, dimensiones



#def extract_medicamentos(path):
 #   df_medicamentos = pd.read_excel(path)
  #  df_medicamentos = df_medicamentos.rename(columns={'Código':'codigo', 'Nombre Genérico':'nombre','Forma Farmacéutica':'forma',
   #                                 'Laboratorio y Registro':'laboratorio', 'Tipo Medicamento':'tipo', 'Presentación':'presentacion','Precio':'precio'})
    #return df_medicamentos



#def extract_receta(conection:Engine):
 #   df_receta = pd.read_sql_query('''select codigo_formula , id_medico, id_usuario, fecha, 
  #  medicamentos_recetados as medicamentos from formulas_medicas''',conection)
   # return df_receta

"""def extract_demografia(conection: Engine):
    df_benco= pd.read_sql_table('cotizante_beneficiario', conection)
    df_cotizantes = pd.read_sql_query(
        '''select cedula as numero_identificacion, tipo_cotizante, estado_civil, sexo, fecha_nacimiento,
            nivel_escolaridad, estracto, proviene_otra_eps,salario_base,tipo_discapacidad,id_ips from cotizante''', conection)
    df_beneficiarios = pd.read_sql_query(
        '''select id_beneficiario as numero_identificacion, fecha_nacimiento, sexo, estado_civil,
         tipo_discapacidad from beneficiario ''', conection)
    df_ips = pd.read_sql_query('select id_ips,municipio,departamento from ips', conection )
    empresa = pd.read_sql_query('select nit , nombre as empresa from empresa', conection)
    empcot = pd.read_sql_query('select empresa as nit, cotizante as numero_identificacion from empresa_cotizante', conection)
    return [df_benco,df_cotizantes,df_beneficiarios,df_ips, empresa, empcot]

def extract_enfermedades(conection : Engine):
    urgencias = pd.read_sql_query('select id_usuario, diagnostico,fecha_atencion from urgencias', conection)
    hospitalizaciones = pd.read_sql_query('select id_usuario, diagnostico, fecha_atencion  from hospitalizaciones', conection)
    citas_generales = pd.read_sql_query('select id_usuario, diagnostico,fecha_atencion  from citas_generales', conection)
    remisiones = pd.read_sql_query('select id_usuario, diagnostico, fecha_atencion  from remisiones', conection)

    return [urgencias, citas_generales, hospitalizaciones, remisiones]

def extract_paymetns(conection: Engine):
    df = pd.read_sql_query('select * from pagos', conection)
    return df

def extract_retiros(conection: Engine,conection_etl):
    df_retiros = pd.read_sql_query('select * from retiros', conection)
    df_pagos = pd.read_sql_query('select * from pagos', conection)
    df_per = pd.read_sql_table('dim_persona', conection_etl)
    df_dom = pd.read_sql_query('select * from dim_demografia', conection_etl)
    df_fecha = pd.read_sql_query('select * from dim_fecha', conection_etl)
    return [df_pagos, df_retiros,df_per, df_dom,df_fecha]




def extract_remisiones(conection : Engine,etl):
    df_demo = pd.read_sql_table('dim_demografia', etl)
    df_persona = pd.read_sql_query('select key_dim_persona, numero_identificacion from dim_persona', etl)
    df_medico = pd.read_sql_query('select key_dim_medico, cedula from  dim_medico',etl)
    df_fecha = pd.read_sql_query('select date, key_dim_fecha  from dim_fecha',etl)
    df_remisiones = pd.read_sql_query('select id_usuario, servicio_pos, id_medico, fecha_remision, codigo_remision from remisiones', conection)
    df_servicio_pos = pd.read_sql_query('select key_dim_servicio, id_servicio_pos servicio_pos,costo from dim_servicio', etl)
    return [df_remisiones, df_servicio_pos,df_persona,df_medico,df_fecha,df_demo]

def extract_servicios(conectionn):
    df_servicios = pd.read_sql_table('servicios_pos', conectionn)
    return df_servicios
"""


#%%
