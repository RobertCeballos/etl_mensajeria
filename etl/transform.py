#%%
import datetime
from datetime import timedelta, date, datetime
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


def transform_sede(dim_sede: DataFrame) -> DataFrame:
    #dim_medico.replace({np.nan: 'no aplica', ' ': 'no aplica','':'no_aplica'}, inplace=True)
    #dim_medico["saved"] = date.today()
    return dim_sede


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
    
    dim_fecha = pd.DataFrame({"fecha": pd.date_range(start='1/9/2023', end='1/9/2024', freq='D')})
    num_filas = 366
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

def transform_hora() -> DataFrame: 

    return dim_hora

def transform_estado_servicio(dim_estado_servicio: DataFrame) -> DataFrame:
    
    return dim_estado_servicio

def transform_novedad(dim_novedad: DataFrame) -> DataFrame:
    dim_novedad=""
    return dim_novedad

def transform_prioridad(dim_prioridad: DataFrame) -> DataFrame:

    return dim_prioridad

def transform_servicio(dim_servicio: DataFrame) -> DataFrame:

    return dim_servicio