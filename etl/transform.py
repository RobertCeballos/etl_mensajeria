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
    return dim_ciudad


def transform_mensajero(dim_mensajero: DataFrame) -> DataFrame:
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

