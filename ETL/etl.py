import pandas as pd
import geopandas as gpd

from shapely.geometry import LineString, Point
from shapely.geometry.polygon import Polygon

import requests
from requests.exceptions import HTTPError
import datetime as dt
from datetime import datetime

import logging
import json

#import psycopg2
#from psycopg2 import Error
from sqlalchemy import create_engine

import ftfy

import os
from dotenv import load_dotenv

# Logging
logging.basicConfig(level=logging.INFO)


# Variables de Entorno
load_dotenv()
asuncion = os.environ.get('asuncion')
user = os.environ.get('user')
passw = os.environ.get('passw')
host = os.environ.get('host')
port = os.environ.get('port')
db = os.environ.get('db')

# Waze Feeds
def waze_feed(url):
    """Request waze json feeds."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Request: Completado.")
        return response
    except HTTPError as http_err:
        logging.error(f'HTTP error {id}: {http_err}')
    except Exception as err:
        logging.critical(f'Error {id}: {err}')

# Exploding por categorias
def normalize(data_dict):
    "Exploding wazze json feeds."
    try:
        df_feed_alerts = pd.json_normalize(data_dict, record_path=['alerts'])
    except (IOError, Exception) as err:
        logging.error(f"Normalize: No se encontraron Alertas. {err}")
    try:
        df_feed_jams = pd.json_normalize(data_dict, record_path=['jams'])
    except (IOError, Exception) as err:
        logging.error(f"Normalize: No se encontraron Jams. {err}")
    try:
        df_feed_irr = pd.json_normalize(data_dict, record_path=['irregularities'])
    except (IOError, Exception) as err:
        logging.info(f"Normalize: No se encontraron Irregularities. {err}")
    return df_feed_alerts, df_feed_jams, df_feed_irr


# -------------- ETL -------------------
def agregar_columnas(gdf, name=None, tipo=None, **kwargs):
  """Agrega columnas time_stamp, Hora, Dia_Sem, Mes."""
  # (tipo= json, post) | (name= Feed Alerts, Feed Jams | Post Alerts, Post Jams)
  # Agregar time_stamp a Feed Jams
  if tipo == 'json':
    # Renombrar columnas para igualar al postgis
    try:
      gdf.rename(columns={'type':'alert_type'}, inplace=True)
    except (IOError, Exception) as err:
      logging.error(f"{name}: Json. Error al cambiar type por alert_type: {err}.")
    # Renombrar pubmillis por time_stamp para igualar al postgis
    if 'time_stamp' not in gdf.columns:
      try:
        gdf.rename(columns={'pubMillis': 'time_stamp'}, inplace=True)
      except (IOError, Exception) as err:
        logging.error(f"{name}: Json. Error al cambiar pubmillis por time_stamp: {err}.")

  # Convertir time_stamp a datetime
  if 'time_stamp' in gdf.columns:
    if gdf.time_stamp.dtype != 'datetime64[ns]':
      if tipo != 'csv':
        def get_date(x):
          return dt.datetime.fromtimestamp(x/1000)
        try:
          gdf['time_stamp'] = gdf['time_stamp'].apply(get_date)
        except (Exception) as err:
          logging.error(f"{name} : No se pudo convertir time_stamp: {err}")
      else:
        try:
          gdf['time_stamp'] = pd.to_datetime(gdf['time_stamp'])
        except Exception as err:
          logging.error(f"{name} : No se pudo asignar datetime64[ns] a time_stamp: {err}")
    # Generar columnas de tiempo.
    try:
      #logging.info(f"{name}: {gdf.time_stamp.dtype}") #########################3 Arreglar query_limit 0
      gdf['Hora'] = gdf.time_stamp.dt.hour
      gdf['Dia_Semana'] = gdf.time_stamp.dt.day_of_week
      gdf['Mes'] = gdf.time_stamp.dt.month
    except (Exception) as err:
      logging.error(f"{name}: agregar_columnas: No se pudo generar (Hora, Dia_Sem, Mes): {err}")
  else:
    if 'detection_date' in gdf.columns:
      try:
        gdf['Hora'] = gdf.detection_date.dt.hour
        gdf['Dia_Semana'] = gdf.detection_date.dt.day_of_week
        gdf['Mes'] = gdf.detection_date.dt.month
      except (Exception) as err:
        logging.error(f"{name}: agregar_columnas: No se pudo generar (Hora, Dia_Sem, Mes): {err}")
    # Generar lat, long
  if name in ["Post Alerts"]:
    try:
      #logging.warning("Name in POST ALERTS lol")
      gdf['location.x'] = gdf.geom.x
      gdf['location.y'] = gdf.geom.y
    except (IOError, Exception) as err:
      logging.warning(f"{name} : No se pudo generar X, Y.{err} ")
  elif name in ['Feed Jams', "Feed Alerts"]:
    try:
      a=1
        #gdf['location.x'] = gdf.location.x
        #gdf['location.y'] = gdf.location.y
    except (IOError, Exception) as err:
      logging.warning(f"{name} : No se pudo generar X, Y.{err} ")
  return gdf

def agregar_dtypes(gdf, name=None, tipo=None, **kwargs):
  """Convierte a dtypes apropiados."""
  # Generar GeoDataFrame primero.
  gdf = gpd.GeoDataFrame(gdf)
  # Diccionario de dtypes a aplicar.
  to_dtype = {'uuid':'string', 'country':'string', 'city':'string', 'street':'string', 'endnode':'string', 'subtype':'string', 'type':'string'}
  for key, value in to_dtype.items():
    try:
      if key in gdf.columns:
        gdf[key] = gdf[key].astype(value)
    except (Exception) as err:
      logging.error(f"{name} : Error al cambiar dtypes.{err} ")
  return gdf

def agregar_geometry(gdf, name=None, tipo="csv", **kwargs):
  """Añade geometry series para jams, según procedencia."""
  if tipo=="csv":
    try:
      gdf['geom'] = gdf.geom.astype('string')
      gdf['geom'] = gpd.GeoSeries.from_wkt(gdf['geom'])
      gdf = gpd.GeoDataFrame(gdf, geometry='geom')
      #.set_crs(4326, allow_override=True, inplace=True)
    except (Exception) as err:
      logging.critical(f"{name} : No se pudo procesar Geometry. {err} ")

  elif tipo=="json":
    if 'line' in gdf.columns:
      try:
        geom = []
        idx=0
        for linestring in gdf['line']:
          line_dict = json.loads(json.dumps(linestring))
          for i in range(len(line_dict)):
            x = json.loads(json.dumps(line_dict[i]['x']))
            y = json.loads(json.dumps(line_dict[i]['y']))
            punto = (float(x), float(y))
            geom.append(punto)
          linestring = LineString(geom)
          geom.clear()
          gdf.iloc[idx, 3] = linestring
          gdf.rename(columns={'line':'geom'}, inplace=True)
          idx = idx+1
      except (IOError, Exception) as err:
          logging.critical(f"{name} : No se pudo generar LineString: {err} ")
  return gdf

def quitar_columnas(gdf, name=None, tipo=None, **kwargs):
  """Eliminar columnas innecesarias"""
  # Lista columnas a eliminar.
  to_drop = ['jam_speed_mph', 'jam_length_ft', 'field_1'] #geometry
  if (tipo == "csv") or (tipo == 'post'):
    for col in to_drop:
      if col in gdf.columns:
        try:
          gdf.drop(col, axis=1, inplace=True)
        except (IOError, Exception) as err:
          logging.error(f"{name} : No se pudo borrar {col}. {err}")
  return gdf

# Limpiar registro de error de encoding.
def ftfy_clean(x):
  """Limpiar errores de codificación"""
  x = ftfy.fix_text(str(x))
  return x

# Limpiar features específicos de errores de encoding.
def clean_str(gdf, name=None, tipo=None, **kwargs):
  """Limpia gdf de errores de encoding."""
  #Lista de elementos a limpiar.
  if tipo!="json":
    to_clean = ['street', 'endnode', 'city']
    for col in gdf.columns:
      if col in to_clean:
        try:
          gdf[col] = gdf[col].apply(lambda x: ftfy_clean(x))
          gdf[col] = gdf[col].str.replace(">","")
        except (Exception) as err:
          logging.error(f"{name} : No se pudo aplicar ftfy (>): {to_clean[col]}.")
  return gdf

"""
    if gdf is None:
      msg = f"{name}:agregar_columnas: Retorno None."
      logging.error(msg)
      print(msg)
    if gdf is None:
      msg = f"{name}:agregar_dtypes: Retorno None."
      logging.error(msg)
      print(msg)
    if gdf is None:
      msg = f"{name}:quitar_columnas: Retorno None."
      logging.error(msg)
      print(msg)
    if gdf is None:
      msg = f"{name}:agregar_geometry: Retorno None."
      logging.error(msg)
      print(msg)
      if gdf is None:
        msg = f"{name}:clean_str: Retorno None."
        logging.error(msg)
        print(msg)"""

# --- FLUJO ----
def preparar_gdf(gdf, name=None, tipo=None, **kwargs):
  """Flujo principal de preparado de GeoDataFrame para su posterior procesado."""
  if gdf is not None:
    gdf = agregar_columnas(gdf, name=name, tipo=tipo)
    gdf = agregar_dtypes(gdf, name=name, tipo=tipo)
    gdf = quitar_columnas(gdf, name=name, tipo=tipo)
    gdf = agregar_geometry(gdf, name=name, tipo=tipo)
    if tipo == "post":
      gdf = clean_str(gdf, name=name, tipo=tipo)
  return gdf


############# DEF RUN_ETL ###############
def run_etl(tipo=None, query_limit=None, desde=None, hasta=None):
    """Consulta de Datos Waze. Tipos: {'json':'API Waze', 'post':'PostGIS'}."""
    gdf_alerts = None
    gdf_jams = None
    gdf_irr = None

    if tipo == None:
      return "Consulta inválida, debe especificar tipo de consulta: json|post"

    ########## INGESTA ################
    if tipo == 'json':
      try:
        # Request Asunción
        req_asuncion = waze_feed(asuncion)
        # Request como diccionario
        data_dict = req_asuncion.json()
      except Exception as err:
        logging.fatal(f"Request: Error al conectar con Waze. {err}")

      # Exploding 
      try:
        df_feed_alerts = pd.json_normalize(data_dict, record_path=['alerts'])
      except (IOError, Exception) as err:
        logging.error(f"Exploding: No se encontraron Alertas. {err}")
      try:
        df_feed_jams = pd.json_normalize(data_dict, record_path=['jams'])
      except (IOError, Exception) as err:
        logging.error(f"Exploding: No se encontraron Jams. {err}")
      try:
        df_feed_irr = pd.json_normalize(data_dict, record_path=['irregularities'])
      except (IOError, Exception) as err:
            logging.info(f"Exploding: No se encontraron Irregularities. {err}")
            df_feed_irr = None

    elif tipo == 'post':
      tomorrow = datetime.today() + dt.timedelta(days=1)
      tomorrow = tomorrow.strftime('%Y-%m-%d')
        # Querys
      if query_limit==0:
        alerts_query = f"SELECT * from w4c.alerts ORDER BY time_stamp DESC" 
        jams_query = f"SELECT * from w4c.detected_jams ORDER BY time_stamp DESC"
        irr_query = f"SELECT * from w4c.irregularities ORDER BY detection_date DESC"
      elif (query_limit != None) and (desde==None) and (hasta==None):
        alerts_query = f"SELECT * from w4c.alerts ORDER BY time_stamp DESC LIMIT {query_limit}"
        jams_query = f"SELECT * from w4c.detected_jams ORDER BY time_stamp DESC LIMIT {query_limit}"
        irr_query = f"SELECT * from w4c.irregularities ORDER BY detection_date LIMIT {query_limit}"
      elif (desde != None) and (hasta != None):
        try:
          desde_date = datetime.strptime(desde, '%Y/%m/%d')
          hasta_date = datetime.strptime(hasta, '%Y/%m/%d')
          if desde_date > hasta_date:
            return "Error de asignación de fechas: Desde>Hasta."
          else:
            alerts_query = f"SELECT * from w4c.alerts WHERE time_stamp > {desde} AND time_stamp < {hasta} ORDER BY time_stamp DESC" 
            jams_query = f"SELECT * from w4c.detected_jams WHERE time_stamp > {desde} AND time_stamp < {hasta} ORDER BY time_stamp DESC"
            irr_query = f"SELECT * from w4c.irregularities WHERE detection_date > {desde} AND detection_date < {hasta} ORDER BY detection_date DESC"
        except (IOError, Exception) as err:
          logging.error(f"Run ETL: Formatos de Fecha no válidos para los parámetros Desde, Hasta.{err} ")
      elif (hasta == None):
        alerts_query = f"SELECT * from w4c.alerts WHERE time_stamp >= '{desde}' AND time_stamp <= '{tomorrow}' ORDER BY time_stamp DESC" 
        jams_query = f"SELECT * from w4c.detected_jams WHERE time_stamp >= '{desde}' AND time_stamp <= '{tomorrow}' ORDER BY time_stamp DESC"
        irr_query = f"SELECT * from w4c.irregularities WHERE detection_date >= '{desde}' AND detection_date <= '{tomorrow}' ORDER BY detection_date DESC"

      # SQLAlchemy
      db_connection_url = f"postgresql://{user}:{passw}@{host}:{port}/{db}"
      sql_engine  = create_engine(db_connection_url)
      conn = sql_engine.raw_connection()

      # Crear GeoDataFrames
      gdf_post_alerts = gpd.read_postgis(alerts_query, conn)
      gdf_post_jams = gpd.read_postgis(jams_query, conn)
      gdf_post_irr = gpd.read_postgis(irr_query, conn)

      conn.close()

    ############### PREPROCESS ####################
    if tipo == 'json':
        try:
            gdf_alerts = preparar_gdf(df_feed_alerts, name="Feed Alerts", tipo=tipo)
            gdf_jams = preparar_gdf(df_feed_jams, name="Feed Jams", tipo=tipo)
            gdf_irr = preparar_gdf(df_feed_irr, name="Feed Irregularities", tipo=tipo)
        except (Exception, IOError) as err:
            logging.fatal(f'Preprocess: Error al preparar geodataframes. {err}')
    elif tipo == 'post':
        try:
            gdf_alerts = preparar_gdf(gdf_post_alerts, name="Post Alerts", tipo=tipo)
            gdf_jams = preparar_gdf(gdf_post_jams, name="Post Jams", tipo=tipo)
            gdf_irr = preparar_gdf(gdf_post_irr, name="Post Irregularities", tipo=tipo)
        except (Exception, IOError) as err:
            logging.fatal(f'Preprocess: Error al preparar geodataframes. {err}')
    return gdf_alerts, gdf_jams, gdf_irr


if __name__ == '__main__':
  gdf_alerts, gdf_jams, gdf_irr = run_etl(tipo='json', desde="2024-04-01")
  
  print("="*64)
  for gdf in [gdf_jams]: #[gdf_alerts, gdf_jams, gdf_irr]:
    try:
      if gdf is not None:
        print("*"*120)
        print(gdf.shape)
        print(gdf.columns)
        print(gdf.head)
      else:
        print("GDF vacío.")
    except (IOError, Exception) as err:
      print(f"Error: {err}.")

  if gdf_alerts is not None:
    try:
      print(gdf_alerts.time_stamp.min(), " - ", gdf_alerts.time_stamp.max())
    except Exception as ex:
      print(f"Error: {ex}.")

        