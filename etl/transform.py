#%%
import datetime
from datetime import timedelta, date, datetime
import sys
from typing import Tuple, Any

import holidays
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, when
from mlxtend.frequent_patterns import apriori
from mlxtend.preprocessing import TransactionEncoder
from pandas import DataFrame


def transform_cliente(dim_cliente: DataFrame) -> DataFrame:

    return dim_cliente


def transform_ciudad(dim_ciudad: DataFrame) -> DataFrame:
    #dim_medico.replace({np.nan: 'no aplica', ' ': 'no aplica','':'no_aplica'}, inplace=True)
    #dim_medico["saved"] = date.today()
    return dim_ciudad


def transform_mensajero(dim_mensajero: DataFrame) -> DataFrame:
    '''beneficiarios, cotizantes, cot_ben = args
    cotizantes.rename(columns={'cedula': 'numero_identificacion'}, inplace=True)
    cotizantes.drop(
        columns=['direccion', 'tipo_cotizante', 'nivel_escolaridad', 'estracto', 'proviene_otra_eps', 'salario_base',
                 'fecha_afiliacion', 'id_ips'], inplace=True)
    cotizantes['tipo_documento'] = "cedula"
    cotizantes['tipo_usuario'] = "cotizante"
    cotizantes['grupo_familiar'] = cotizantes['numero_identificacion']
    beneficiarios.drop(columns=['parentesco'], inplace=True)
    beneficiarios.rename(columns={'tipo_identificacion': 'tipo_documento', 'id_beneficiario': 'numero_identificacion'},
                         inplace=True)
    beneficiarios['tipo_usuario'] = "beneficiario"
    beneficiario = beneficiarios.merge(cot_ben, left_on='numero_identificacion', right_on='beneficiario', how='left')
    beneficiario.rename(columns={'cotizante': 'grupo_familiar'}, inplace=True)
    beneficiario.drop(columns=['beneficiario'], inplace=True)
    dim_persona = pd.concat([beneficiario, cotizantes])
    dim_persona["saved"] = date.today()
    dim_persona.reset_index(drop=True, inplace=True)'''

    return dim_mensajero

def transform_fecha() -> DataFrame:
    
    dim_fecha = pd.DataFrame({"fecha": pd.date_range(start='9/1/2023', end='9/1/2024', freq='D')})
    num_filas = 367
    dim_fecha["fecha_id"] = range(1, num_filas + 1)
    dim_fecha["ano"] = dim_fecha["fecha"].dt.year
    dim_fecha["mes"] = dim_fecha["fecha"].dt.month
    dim_fecha["dia_mes"] = dim_fecha["fecha"].dt.day
    dim_fecha["dia_semana"] = dim_fecha["fecha"].dt.weekday
    co_holidays = holidays.CO(language="es")
    dim_fecha["festivo"] = dim_fecha["fecha"].apply(lambda x: x in co_holidays)
    dim_fecha["festivo_nombre"] = dim_fecha["fecha"].apply(lambda x: co_holidays.get(x))
    return dim_fecha

""""
def transform_fecha() -> DataFrame:
    dim_fecha = pd.DataFrame({"date": pd.date_range(start='1/1/2005', end='1/1/2009', freq='D')})
    dim_fecha["year"] = dim_fecha["date"].dt.year
    dim_fecha["month"] = dim_fecha["date"].dt.month
    dim_fecha["day"] = dim_fecha["date"].dt.day
    dim_fecha["weekday"] = dim_fecha["date"].dt.weekday
    dim_fecha["quarter"] = dim_fecha["date"].dt.quarter
    dim_fecha["day_of_year"] = dim_fecha["date"].dt.day_of_year
    dim_fecha["day_of_month"] = dim_fecha["date"].dt.days_in_month
    dim_fecha["month_str"] = dim_fecha["date"].dt.month_name()  # run locale -a en unix
    dim_fecha["day_str"] = dim_fecha["date"].dt.day_name()  # locale = 'es_CO.UTF8'
    dim_fecha["date_str"] = dim_fecha["date"].dt.strftime("%d/%m/%Y")
    co_holidays = holidays.CO(language="es")
    dim_fecha["is_Holiday"] = dim_fecha["date"].apply(lambda x: x in co_holidays)
    dim_fecha["holiday"] = dim_fecha["date"].apply(lambda x: co_holidays.get(x))
    dim_fecha["weekend"] = dim_fecha["weekday"].apply(lambda x: x > 4)
    dim_fecha["saved"] = date.today()
    return dim_fecha
"""

def transform_hora():
    horas = []
    hora_id = 1

    for h in range(24):
        for m in range(60):
            hora_str = f"{h:02}:{m:02}"
            
            # Categorizar hora en segmentos del día
            if 6 <= h < 12:
                periodo = "Mañana"
            elif 12 <= h < 18:
                periodo = "Tarde"
            elif 18 <= h < 24:
                periodo = "Noche"
            else:
                periodo = "Madrugada"
            
            # Horas pico definidas arbitrariamente
            es_pico = h in [7, 8, 12, 17, 18]

            horas.append({
                'hora_id': hora_id,
                'hora': h,
                'minuto': m,
                'hora_str': hora_str,
                'periodo_dia': periodo,
                'es_hora_pico': es_pico
            })
            hora_id += 1

    return pd.DataFrame(horas)

def transform_estado_servicio(dim_estado_servicio: DataFrame) -> DataFrame:
    
    return dim_estado_servicio

def transform_novedad(dim_novedad: DataFrame) -> DataFrame:

    return dim_novedad

def transform_prioridad(dim_prioridad: DataFrame) -> DataFrame:

    return dim_prioridad

def transform_servicio(dim_servicio: DataFrame) -> DataFrame:

    return dim_servicio

def transform_hecho_solicitud_servicios(hecho_solicitud_servicios: DataFrame) -> DataFrame:
# Desempaquetar los DataFrames por índice
    df_servicio = hecho_solicitud_servicios[0].copy()   # dim_servicio
    df_hora = hecho_solicitud_servicios[1]              # dim_hora
    df_fecha = hecho_solicitud_servicios[2]             # dim_fecha
    df_prioridad = hecho_solicitud_servicios[5]         # dim_prioridad

    # === Join con dim_fecha para obtener fecha_solicitud_id
    df = df_servicio.merge(df_fecha, left_on='fecha_solicitud', right_on='fecha', how='left')
    df.rename(columns={'fecha_id': 'fecha_solicitud_id'}, inplace=True)

    # === Extraer hora y minuto desde hora_solicitud y unir con dim_hora
    df['hora'] = df['hora_solicitud'].apply(lambda x: x.hour)
    df['minuto'] = df['hora_solicitud'].apply(lambda x: x.minute)

    df = df.merge(df_hora, on=['hora', 'minuto'], how='left')
    df.rename(columns={'hora_id': 'hora_solicitud_id'}, inplace=True)

    # === Join con dim_prioridad para obtener prioridad_id
    df = df.merge(df_prioridad, on='prioridad', how='left')

    # === Seleccionar columnas finales
    df_hecho = df[[
        'id',                  # servicio_id
        'cliente_id',
        'ciudad_origen_id',
        'fecha_solicitud_id',
        'hora_solicitud_id',
        'prioridad_id'
    ]].copy()

    df_hecho.rename(columns={'id': 'servicio_id'}, inplace=True)
    return df_hecho

def transform_hecho_ejecucion_servicios(raw_df, dimensiones):
    # Asegurar formatos compatibles para merge
    raw_df['fecha'] = pd.to_datetime(raw_df['fecha']).dt.normalize()
    dimensiones['dim_fecha']['fecha'] = pd.to_datetime(dimensiones['dim_fecha']['fecha']).dt.normalize()
    
    # Extraer hora y minuto
    raw_df['hora'] = pd.to_datetime(raw_df['hora'], format='%H:%M:%S', errors='coerce')
    raw_df['hora_num'] = raw_df['hora'].dt.hour
    raw_df['minuto_num'] = raw_df['hora'].dt.minute

    # Renombrar estado_id
    raw_df = raw_df.rename(columns={'estado_id': 'estado_servicio_id'})

    # Mapear fecha_estado_id desde dim_fecha
    raw_df = raw_df.merge(dimensiones['dim_fecha'], how='left', left_on='fecha', right_on='fecha')
    raw_df.rename(columns={'fecha_id': 'fecha_estado_id'}, inplace=True)

    # Mapear hora_estado_id desde dim_hora (sin segundos)
    raw_df = raw_df.merge(
        dimensiones['dim_hora'],
        how='left',
        left_on=['hora_num', 'minuto_num'],
        right_on=['hora', 'minuto']
    )
    raw_df.rename(columns={'hora_id': 'hora_estado_id'}, inplace=True)

    # Filtrar servicios válidos
    servicios_validos = dimensiones['dim_servicio']['id'].unique()
    raw_df = raw_df[raw_df['servicio_id'].isin(servicios_validos)]

    # Mapear mensajero_id desde dim_mensajero
    # Asumo que raw_df tiene la columna 'mensajero_username' que coincide con dim_mensajero.username
    raw_df = raw_df.merge(
        dimensiones['dim_mensajero'][['id', 'username']],
        how='left',
        left_on='mensajero_username',   # Ajusta esta columna si es otro campo
        right_on='username'
    )
    raw_df.rename(columns={'id': 'mensajero_id'}, inplace=True)
    raw_df.drop(columns=['username'], inplace=True)

    # Mapear novedad_id desde dim_novedad
    # Asumo que raw_df tiene la columna 'novedad_nombre' que coincide con dim_novedad.nombre
    raw_df = raw_df.merge(
        dimensiones['dim_novedad'][['id', 'nombre']],
        how='left',
        left_on='novedad_nombre',       # Ajusta esta columna si es otro campo
        right_on='nombre'
    )
    raw_df.rename(columns={'id': 'novedad_id'}, inplace=True)
    raw_df.drop(columns=['nombre'], inplace=True)

    # Selección final de columnas
    final_df = raw_df[[
        'servicio_id',
        'estado_servicio_id',
        'fecha_estado_id',
        'hora_estado_id',
        'mensajero_id',
        'novedad_id'
    ]].dropna(subset=[
        'servicio_id', 'estado_servicio_id', 'fecha_estado_id', 'hora_estado_id'
    ])

    return final_df

def transform_hecho_ejecucion_servicios(raw_df, dimensiones):
    import pandas as pd

    # Asegurar formatos compatibles para merge
    raw_df['fecha'] = pd.to_datetime(raw_df['fecha']).dt.normalize()
    dimensiones['dim_fecha']['fecha'] = pd.to_datetime(dimensiones['dim_fecha']['fecha']).dt.normalize()

    # Extraer hora y minuto
    raw_df['hora'] = pd.to_datetime(raw_df['hora'], format='%H:%M:%S', errors='coerce')
    raw_df['hora_num'] = raw_df['hora'].dt.hour
    raw_df['minuto_num'] = raw_df['hora'].dt.minute

    # Renombrar columnas
    raw_df = raw_df.rename(columns={
        'estado_id': 'estado_servicio_id',
        'mensajero_id': 'mensajero_id',
        'tipo_novedad_id': 'novedad_id'
    })

    # Mapear fecha_estado_id
    raw_df = raw_df.merge(dimensiones['dim_fecha'], how='left', left_on='fecha', right_on='fecha')
    raw_df.rename(columns={'fecha_id': 'fecha_estado_id'}, inplace=True)

    # Mapear hora_estado_id
    raw_df = raw_df.merge(
        dimensiones['dim_hora'],
        how='left',
        left_on=['hora_num', 'minuto_num'],
        right_on=['hora', 'minuto']
    )
    raw_df.rename(columns={'hora_id': 'hora_estado_id'}, inplace=True)

    # Filtrar servicios válidos
    servicios_validos = dimensiones['dim_servicio']['id'].unique()
    raw_df = raw_df[raw_df['servicio_id'].isin(servicios_validos)]

    # Validar mensajero_id
    mensajeros_validos = set(dimensiones['dim_mensajero']['id'])
    raw_df.loc[~raw_df['mensajero_id'].isin(mensajeros_validos), 'mensajero_id'] = None

    # Validar novedad_id
    novedades_validas = set(dimensiones['dim_novedad']['id'])
    raw_df.loc[~raw_df['novedad_id'].isin(novedades_validas), 'novedad_id'] = None

    # === NUEVO: Calcular tiempo por fase ===
    # Crear timestamp combinando fecha y hora
    raw_df['timestamp'] = pd.to_datetime(raw_df['fecha']) + pd.to_timedelta(raw_df['hora_num'], unit='h') + pd.to_timedelta(raw_df['minuto_num'], unit='m')

    # Ordenar por servicio y timestamp
    raw_df = raw_df.sort_values(by=['servicio_id', 'fecha_estado_id', 'hora_estado_id'])

    # Calcular duración por fase (en minutos)
    raw_df['tiempo_fase_min'] = raw_df.groupby('servicio_id')['timestamp'].diff().dt.total_seconds() / 60

    # Si quieres: rellenar NA con 0 o dejar como NaN según uso
    raw_df['tiempo_fase_min'] = raw_df['tiempo_fase_min'].fillna(0)

    # Selección final de columnas
    final_df = raw_df[[
        'servicio_id',
        'estado_servicio_id',
        'fecha_estado_id',
        'hora_estado_id',
        'mensajero_id',
        'novedad_id',
        'tiempo_fase_min'  # nueva columna agregada
    ]].dropna(subset=[
        'servicio_id', 'estado_servicio_id', 'fecha_estado_id', 'hora_estado_id'
    ])

    return final_df

