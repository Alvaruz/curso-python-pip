import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame

from sqlalchemy import create_engine

from dotenv import load_dotenv

import os
from datetime import datetime
import datetime as dt

class DatosPostgres():
    def __init__(self) -> None:
        load_dotenv()
        #self.db = db
        self.asuncion = os.environ.get('asuncion')
        self.user = os.environ.get('user')
        self.passw = os.environ.get('passw')
        self.host = os.environ.get('host')
        self.port = os.environ.get('port')
        self.db = os.environ.get('db')

        # SQLAlchemy
        self.db_connection_url = f"postgresql://{self.user}:{self.passw}@{self.host}:{self.port}/{self.db}"
        self.sql_engine  = create_engine(self.db_connection_url)
        self.conn = self.sql_engine.raw_connection()
    
    def get_data(self, desde:str=None, hasta:str=None, query_limit:int=5000) -> GeoDataFrame:
        # Consultas
        if desde and not hasta:
            tomorrow = datetime.today() + dt.timedelta(days=1)
            tomorrow = tomorrow.strftime('%Y-%m-%d')

            alerts_query = f"SELECT * from w4c.alerts WHERE time_stamp >= '{desde}' AND time_stamp <= '{tomorrow}' ORDER BY time_stamp DESC" 
            jams_query = f"SELECT * from w4c.detected_jams WHERE time_stamp >= '{desde}' AND time_stamp <= '{tomorrow}' ORDER BY time_stamp DESC"
            irr_query = f"SELECT * from w4c.irregularities WHERE detection_date >= '{desde}' AND detection_date <= '{tomorrow}' ORDER BY detection_date DESC"
        
        elif desde and hasta:
            alerts_query = f"SELECT * from w4c.alerts WHERE time_stamp > {desde} AND time_stamp < {hasta} ORDER BY time_stamp DESC" 
            jams_query = f"SELECT * from w4c.detected_jams WHERE time_stamp > {desde} AND time_stamp < {hasta} ORDER BY time_stamp DESC"
            irr_query = f"SELECT * from w4c.irregularities WHERE detection_date > {desde} AND detection_date < {hasta} ORDER BY detection_date DESC"

        else:
            alerts_query = f"SELECT * from w4c.alerts ORDER BY time_stamp DESC LIMIT {query_limit}"
            jams_query = f"SELECT * from w4c.detected_jams ORDER BY time_stamp DESC LIMIT {query_limit}"
            irr_query = f"SELECT * from w4c.irregularities ORDER BY detection_date LIMIT {query_limit}"

        # Crear GeoDataFrames
        gdf_post_alerts = gpd.read_postgis(alerts_query, self.conn)
        gdf_post_jams = gpd.read_postgis(jams_query, self.conn)
        gdf_post_irr = gpd.read_postgis(irr_query, self.conn)
        self.conn.close()

        return gdf_post_alerts,gdf_post_jams, gdf_post_irr

if __name__ == '__main__':
    alerts, jams, irr = DatosPostgres().get_data(desde='2024-04-26')
    print(jams.head())