import requests
from requests.exceptions import HTTPError

import pandas as pd
from pandas import DataFrame

from dotenv import load_dotenv

import os
import datetime as dt
import time

import logging
today = dt.datetime.today()

filename = f"logs/{today.day:02d}-{today.month:02d}-{today.year}.log"

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("Waze_Service")

file_handler = logging.FileHandler(filename) #waze.log
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s: %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

class DatosWaze():
    def __init__(self) -> None:
        load_dotenv()
        self.asuncion = os.environ.get('asuncion')

    # Waze Feeds
    def waze_feed(self, url):
        """Request waze json feeds."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            logging.info("Request: Completado.")
            return response
        except HTTPError as http_err:
            logging.error(f'HTTP error {id}: {http_err}')
        except Exception as err:
            logging.error(f'Error {id}: {err}')

    # Exploding por categorias
    def normalize(self, data_dict):
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

"""
    def get_data(sefl) -> DataFrame:
      try:
        # Request Asunción
        req_asuncion = waze_feed(self.asuncion)
        # Request como diccionario
        data_dict = req_asuncion.json()
      except Exception as err:
        logging.error(f"Request: Error al conectar con Waze. {err}")

      # Exploding 
      try:
        df_feed_alerts = pd.json_normalize(data_dict, record_path=['alerts'])
      except (IOError, Exception) as err:
        logging.info(f"Exploding: No se encontraron Alertas. {err}")
      try:
        df_feed_jams = pd.json_normalize(data_dict, record_path=['jams'])
      except (IOError, Exception) as err:
        logging.info(f"Exploding: No se encontraron Jams. {err}")
      try:
        df_feed_irr = pd.json_normalize(data_dict, record_path=['irregularities'])
      except (IOError, Exception) as err:
            logging.info(f"Exploding: No se encontraron Irregularities. {err}")
            df_feed_irr = None

"""

def run():
    logger.debug("Debug")
    logger.info("Info")
    time.sleep(2)
    logger.warning("Warning")
    logger.error("Error")
    time.sleep(2)
    logger.critical("Critical")
    return

if __name__ == '__main__':
    run()