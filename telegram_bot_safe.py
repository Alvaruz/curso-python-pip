#from email import message
#from readline import append_history_file

# Mensajería
import telebot
from telebot import types

# Data 
import pandas as pd
import numpy as np

# Python
import requests
import logging
import json
import os
import pytz
from datetime import datetime, tzinfo
import schedule, threading, time
import subprocess

# ETL
from etl import *
from equipos import semaforos_py

# Logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)

###########################################
###########################################
# ------------ GLOBALES ---------------
telegram = os.environ.get('telegram_prueba') #telegram
server_ip = os.environ.get('server_ip')
HACIENDO_PING = False
ALVARO = os.environ.get('ALVARO')
VICTOR = os.environ.get('VICTOR')
BOT_REPORTS =  os.environ.get("ALVARO") #BOT_REPORTS
CONGESTIONAMIENTOS = os.environ.get("CONGESTIONAMIENTOS")
SEMAFOROS = BOT_REPORTS
ACCIDENTES = os.environ.get("ACCIDENTES")

streets_atms = ['Av. Mcal. López','Av. Fernando de la Mora', 'Av. Eusebio Ayala', 'Av. Perú',  \
'PY-03',  'Av. España', 'Avda. España', 'Av. Félix Bogado', 'Av. Gral. Artigas', \
    'Av. Primer Presidente', 'Av. Aviadores del Chaco', "Av. Próceres de Mayo" ]

DEBUG = False

NOTIFICADOS = []

PRUEBA = 0
BUSY = False
PAUSE = False
PAUSE_MIN = 10

INFORMES_ACTUALES = True
INFORMES_DIA = True
ESTADO_SEMAFOROS = False
EVENTOS_CERCANOS = True

AUTORIZADOS = [ALVARO, VICTOR, 'Alvaro_Cardenas', 'Victorpenhatorres']
LIMITADOS = []
CONOCIDOS = []
init_time = datetime.now()

###########################################
###########################################

try:
    bot = telebot.TeleBot(str(telegram), parse_mode="HTML") # MarkdownV2
except (IOError, Exception) as err:
    logging.fatal(f"Error al conectar a Telegram: {err}.")

#bot.send_message(BOT_REPORTS, "Reiniciando Bot...")

def tiempo():
    """Devuelve hoy(today), hoy_str(str), tz_hoy(timezone)"""
    hoy = datetime.today() #.replace(hour=0,minute=0,second=0,microsecond=0)
    hoy_str = hoy.strftime("%Y-%m-%d")

    tz = pytz.timezone('America/Asuncion')
    tz_hoy = datetime.now(tz)
    return hoy, hoy_str, tz_hoy

def make_eventos():
    """Llamada que genera eventos"""
    url = f"http://{server_ip}:8080/distance2"
    distance2 = requests.get(url)
    #loads_distance2 = json.loads(distance2.text)
    return distance2

def get_eventos():
    """API Call to Django REST for Eventos"""
    if not DEBUG:
        make_eventos()
        url = f"http://{server_ip}:8080/api/eventos/"
        eventos = requests.get(url)
        try:
            loads_eventos = json.loads(eventos.text)
            return loads_eventos
        except (IOError, Exception) as err:
            logging.error(f"No se pudo consultar eventos:  {err}")
    else:
        try:
            with open("eventos.json", 'r') as f:
                return json.load(f)
        except (IOError, Exception) as err:
            logging.error(f"No se pudo generar eventos: {err}")

def get_semaforos():
    """API Call to Django REST for Semaforos"""
    url = f"http://{server_ip}:8080/api/semaforos/"
    semaforos = requests.get(url)
    loads_semaforos = json.loads(semaforos.text)
    return loads_semaforos

def get_emoji(estado):
    """Get Emojis for Semaforos status."""
    if estado in ['ONLINE', 0]:
        return  u'✅' #'\xE2\x9C\x85'
    elif estado in ['DESC', 1]:
        return u'✖️' #'\xE2\x9D\x94'
    else:
        return u'✖️' #u'🔴' #'\xE2\x9D\x8C'

def get_equipos():
    """Obtiene semaforos y otros de csv"""
    try:
        equipos = pd.read_csv("kml/equipos.csv", delimiter=";")
        equipos['Estado'] = np.NaN
        equipos['Cantidad'] = 0
        return equipos
    except (IOError, Exception) as err:
        logging.error(f"Error al Cargar CSV: {err}.")
        try:
            equipos = semaforos_py
        except (IOError, Exception) as err:
            logging.error(f"Error al Cargar Equipo de Archvo PY: {err}.")
        cols  = ["Cruce", "IP_Controlador"]
        equipos = pd.DataFrame(equipos.items(), columns=cols)
        print(equipos)
        equipos['Cantidad'] = 0
    return equipos

# Listado de Semáforos por estado DJANGO
def semaforos(message=BOT_REPORTS):
    """API call for Semaforos in Django Rest.
    Devuelve lista de semaforos con resumen por estado"""
    if not message:
        message = BOT_REPORTS
    response = ''
    # Obtiene listado de semáfors
    semaforos = get_semaforos()
    en_linea = 0
    offline = 0
    falla = 0
    alert = 0
    for semaforo in semaforos:
        if semaforo['tipo'] == 'ATMS':
            estado = semaforo['estado']
            # Respuesta con emoji según estado
            resp = f"- {semaforo['direccion']:<10} {get_emoji(estado)}\n"
            response += resp
            if estado == 'ONLINE':
                en_linea=en_linea+1
            elif estado == 'DESC':
                offline=offline+1
            else:
                falla=falla+1
    bot.send_message(message.chat.id, "Lista de Semáforos...")
    bot.send_message(message.chat.id, response)
    bot.send_message(message.chat.id, \
        f"✅ En Línea: {en_linea}\n✖️ Offline: {offline}\n⚠️ Alerta (Waze): {alert}\n🔴 Falla: {falla}\n")
    timer = datetime.now().strftime('%d/%m/%Y %H:%M')
    logging.info(f"Listado. {timer}.")

def to_bold(var):
    """Negrita para parse HTML"""
    return '<strong>'+str(var)+'</strong>'

# WAZE
def get_waze_data(periodo=None):
    """ETL from Waze.
    Devuelve Dataframe de Alertas."""
    # Variables de tiempo
    hoy, hoy_str, tz_hoy = tiempo()
    if periodo == "Actual":
        try:
            # Obtener dataframes (Api Waze)
            df_alerts, df_jams, df_irr = run_etl(tipo='json', desde=hoy_str)
        except (IOError, Exception) as err:
            logging.error(f"Error al consultar los datos de la API de Waze: {err}.")
    elif periodo == hoy_str:
        try:
            # Obtener dataframes (Postgresql)
            df_alerts, df_jams, df_irr = run_etl(tipo='post', desde=hoy_str)
        except (IOError, Exception) as err:
            logging.error(f"Error al consultar los datos del PostGIS: {err}.")
    # Filtrado == Asunción
    df_alerts = df_alerts[df_alerts.city=="Asunción"]
    timer = datetime.now().strftime('%d/%m/%Y %H:%M')
    logging.info(f"Listado. {timer}.")
    return df_alerts

def collect_raw_data(df, search, schedule=None):
    """ - Filtra Dataframe según el Evento especificado (search).
        - Añade Eventos a Notificados.
    """
    global NOTIFICADOS

    def notificar(df):
        """Añadir uuid's de eventos a listado NOTIFICADOS"""
        global NOTIFICADOS
        # Mantenimiento de lista
        if len(NOTIFICADOS) > 20:
            NOTIFICADOS.clear()
        # Añadir indices notificados
        for idx, row in df.iterrows():
            if row['uuid'] is not None:
                NOTIFICADOS.append(row['uuid'])

    lista = ''
    try:
        if df['alert_type'].str.contains(search).sum():
            # df_res de eventos por tipo
            df_res = df[df['alert_type'] == search]
            # df_res de eventos que no hayan sido notificados
            df_res = df_res[~df_res['uuid'].isin(NOTIFICADOS)]
            # Listado de calles opcional)
            lista = df_res.street.value_counts().to_dict()
            if schedule:
                notificar(df_res)
            return lista, df_res
        elif df['subtype'].str.contains(search).sum():
            df_res = df[df['subtype'] == search]
            df_res = df_res[~df_res['uuid'].isin(NOTIFICADOS)]
            lista = df_res.street.value_counts().to_dict()
            if schedule:
                notificar(df_res)
            return lista, df_res
        else:
            logging.error(f"No encontró alerta. Search: {search}")
            return None, None
    except (Exception, IOError) as err:
        logging.error(f"Error: {err}")
    timer = datetime.now().strftime('%d/%m/%Y %H:%M')
    logging.info(f"Listado. {timer}.")
    return None

def send_custom_message(lista, title, message=None, destino=None):
    """Envia lista resumen."""
    if destino:
        response = ''
        bot.send_message(destino, title)
        for key, value in lista.items():
            resp = key + " : " + str(value) +"\n"
            response +=resp
        bot.send_message(destino, response)
    timer = datetime.now().strftime('%d/%m/%Y %H:%M')
    logging.info(f"Listado. {timer}.")

def resumen(message=None, periodo="Actual", destino=None, schedule=None):
    """Envia Resumen de Eventos:
        - message: obj message
        - periodo: Actual | hoy_str
        - destino: chat.id destino, prioridad sobre message.
        - schedule: indica si es programado.
        """
    hoy, hoy_str, tz_hoy = tiempo()

    # Elección de destino
    if not destino:
        try:
            destino = message.chat.id
        except (IOError, Exception) as err:
            logging.error(f"Error al seleccionar destinatario: {err}.")
    
    # Encabezado
    if periodo=='Actual':
        encabezado = to_bold("--EVENTOS ACTUALES--\n")
    elif periodo==hoy_str:
        encabezado = to_bold(f"--EVENTOS DEL DÍA: {hoy_str}--\n")
    encabezado = encabezado + to_bold("RESUMEN GENERAL:")
    if not schedule:
        bot.send_message(destino, encabezado)

    # Definir alertas segun si es programado o no
    if schedule:
        alerts_dict = {
        'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT': "# Falla de Semáforos:",
        #'ACCIDENT': "# Accidentes:",
        }
    else:
        alerts_dict = {
        'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT': "# Falla de Semáforos:",
        'ACCIDENT': "# Accidentes:",
        'HAZARD_ON_ROAD_CAR_STOPPED': "# Vehículos Detenidos:",
        'HAZARD_ON_ROAD_LANE_CLOSED': "# Calles Cerradas:",
        'HAZARD_ON_ROAD_POT_HOLE': "# Baches:",
        "HAZARD_WEATHER_FLOOD":"# Inundaciones: ",
        }
    tab = "- " #u"&nbsp;&nbsp;&nbsp;&nbsp;"

    # Obtener dataframe de Alertas.
    df_alerts = get_waze_data(periodo=periodo)
    for alert, name in alerts_dict.items():
        try:
            # Filtrado de dataframe(df_res) y resumen(lista). Eventos añadidos a NOTIFICADOS.
            lista, df_res = collect_raw_data(df_alerts, alert, schedule)
        except (IOError, Exception) as err:
            logging.error(f"Error al filtrar datos: {err}")
        logging.info(f"Colected data for {alert}.")
        # Si hay lista(resumen)
        if lista:
            try:
                logging.info(f'{name} Hay lista (resumen)')
                # Titulo para el tipo de alerta.
                
                # Enviar lista(resumen) por telegram.
                send_custom_message(destino=destino, lista=lista, title=to_bold(name))
                # Enviar ubicaciones de evento si estan dentro de filtro.
                if alert in ['HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT', 'ACCIDENT', \
                    'HAZARD_ON_ROAD_LANE_CLOSED', 'HAZARD_WEATHER_FLOOD']:
                    # Limite para enviar ubicaciones
                    loc_limit = 10
                    if df_res.shape[0] > loc_limit:
                        bot.send_message(destino, \
                            f"Ubicaciones de los primeros {loc_limit} eventos.\n")
                    enviados = 0
                    for idx, row in df_res.iterrows():
                        if enviados < loc_limit:
                            bot.send_location(destino, \
                                latitude= row['location.y'], \
                                longitude= row['location.x'] )
                            enviados = enviados+1
                #bot.send_message(destino, "-"*60)
            except (IOError, Exception) as err:
                logging.error(f"Error en procesamiento de Lista: {err}.")
                bot.send_message(destino, "Error Interno.")
        else:
            try:
                logging.info(f"{name} Ningún evento:")
                # Mensajes cuando no hay eventos detectados (solo si no es programado.)
                if not schedule:
                    bot.send_message(destino, to_bold(name))
                    bot.send_message(destino, tab+"No se reportan eventos activos.")
            except (IOError, Exception) as err:
                logging.error(f"Error en procesamiento de Lista VACIA: {err}.")
                bot.send_message(destino, "Error Interno.")

def get_eventos_cercanos(message=False, schedule=False, estado=EVENTOS_CERCANOS):
    """API call a Django Rest. Busca y envia eventos."""
    if schedule == True or not message:
        destinatario = BOT_REPORTS #ALVARO 
    else:
        destinatario = message.chat.id

    # Optener Eventos de la API de Django
    eventos = get_eventos()

    if eventos or (eventos is not None):
        # --- Eventos a DataFrame ---
        # Json exploding
        try:
            # Exploding en dataframe
            eventos = pd.json_normalize(eventos, record_path="semaforo", meta=['uuid', 'tipo', 'created', 'street', 'latitud', 'longitud'], meta_prefix="evento_")
            eventos_n = pd.json_normalize(eventos['evento_tipo'])
            #print("eventos_n: ", eventos_n)
            # Concatenación de eventos + dataframe temporal (datos faltantes)
            eventos = pd.concat([eventos.drop(columns=['evento_tipo']), eventos_n], axis=1)
        except Exception as err:
            logging.info(f"No se encontraron fallas de semaforo.")
        try:
            # Asignación de dtypes en dataframes
            eventos['updated'] = pd.to_datetime(eventos.updated)
            eventos['evento_created'] = pd.to_datetime(eventos.evento_created)

            # Enriquecer uuid con tipo de evento y fecha
            eventos['evento_uuid'] = eventos['evento_uuid'].str.cat([eventos['nombre'],eventos['evento_created'].dt.strftime('%H:%M')], sep="|")
            #print(eventos)
        except (IOError, Exception) as err:
            msg_error = f"Error al configurar datos de la API Eventos: {err}"
            logging.error(msg_error)
            #bot.send_message(ALVARO, msg_error)

        # --- NOTIFICACION -----
        def notificar(df):
            """Añadir uuid's de eventos a listado NOTIFICADOS"""
            global NOTIFICADOS
            # Mantenimiento de lista
            if len(NOTIFICADOS) > 500:
                NOTIFICADOS.clear()
            # Añadir indices notificados
            for idx, row in df.iterrows():
                #print(row)
                if row['evento_uuid'] is not None:
                    NOTIFICADOS.append(row['evento_uuid'])

        def filtrar(df, schedule=schedule):
            """ - Filtra Dataframe según el Evento especificado (search).
                - Añade Eventos a Notificados.
            """
            global NOTIFICADOS

            try:
                """
                print("***********************************************************")
                print("DATAFRAME")
                print(df)
                print("***********************************************************")
                print("Notificados: ", NOTIFICADOS)
                print("---- Hora: ", time.strftime('%d-%m-%Y %H:%M:%S', time.localtime()))"""
                #if len(NOTIFICADOS) > 0:
                if schedule:
                    try:
                        df = df[~df['evento_uuid'].isin(NOTIFICADOS)]
                        notificar(df)
                    except (IOError, Exception) as err:
                        logging.error(f"Error en notificacion {err}.")
                return df
            except (IOError, Exception) as err:
                print(f"Error en filtrado {err}.")

        def filtrar_eventos_viejos(df, schedule=schedule):
            if schedule:
                def borrar_index(df, idx):
                    try:
                        df = df.drop(index=idx)
                        return df
                    except (IOError, Exception) as err:
                        logging.error(f"No se pudo borrar el índice {idx}: {err}")
                        return df

                for idx, row in eventos.iterrows():
                    fecha_evento = row['evento_created'].tz_localize(None)
                    #print("Evento: ", row['evento_created']," Horas desde:",pd.Timedelta(datetime.now() - fecha_evento).seconds/60/60, \
                    #    "Index: ", idx)

                    if (pd.Timedelta(datetime.now() - fecha_evento).days >= 1) or \
                        ((pd.Timedelta(datetime.now() - fecha_evento).seconds/60/60) >= 1):
                        df = borrar_index(df, idx)
            return df

        ####### Principal ##########
        try:
            eventos = filtrar(eventos, schedule=schedule)
            eventos = filtrar_eventos_viejos(eventos, schedule=schedule)
        except (IOError, Exception) as err:
            logging.error(f"No se pudo filtrar DataFrame. {err}")

        # Notificaciones
        resp = ''
        for idx, row in eventos.iterrows():
            #print("IDX: ", idx)
            #print("ROW: ", row)
            if row['nombre'] == "Accidente": # and not schedule
                if row['evento_street'] in streets_atms:
                    "Notifica si esta dentro de las Avenidas de ATMS."
                    resp = resp +'Accidente '+ u'🚙' + '.\n'
                    resp = resp +"Calle: "+ row['evento_street']+'.\n'
                    resp = resp +"Dirección: "+ row['direccion']+'.\n'
                    resp = resp + "Generado: "+str(row['evento_created'])[:16]+'.\n' #[:16]
                    if schedule:
                        bot.send_message(ACCIDENTES, resp)
                    else:
                        bot.send_message(destinatario, resp)

                    # Redis
                    #mapping = {"Calle":  row['evento_street'], "Dirección": row['direccion'],"Generado": str(row['evento_created'])[:16]}
                    #r.hmset(name="Accidentes", mapping=mapping)
                    #ttl = 60*60 # 1 hora
                    #r.expire(name="Accidentes", time=ttl)
                    try:
                        if schedule:
                            bot.send_location(ACCIDENTES, latitude=row['evento_latitud'], longitude=row['evento_longitud'])
                        else:
                            bot.send_location(destinatario, latitude=row['evento_latitud'], longitude=row['evento_longitud'])
                    except (IOError, Exception) as err:
                        logging.error(f"No se pudo enviar ubicación de evento: {err}.")
                    resp = ''
            elif row['nombre'] == "Falla de Semáforo":
                resp = resp + to_bold(row['nombre'])
                resp = resp +' '+u'🚦' + '.\n'
                # if (row['tipo'] == "SEMAFOROS") or (row['tipo'] == "OTROS"):  ---- ESPECIFICAR DEPENDENCIA
                #     resp = resp +"Dependencia: "+ row['tipo']+" "+u'⚪️'+'.\n'
                # else:
                #    resp = resp +"Dependencia: "+ row['tipo']+" "+u'🔴'+'.\n' #u'🚦'🔴⚪️
                resp = resp +"Dirección: "+ row['direccion']+'.\n'
                resp = resp + "Generado: "+str(row['evento_created'])[:16]+'.\n' #[:16]
                bot.send_message(destinatario, resp)
                # Redis
                #mapping = {"Dependencia":  row['tipo'], "Dirección": row['direccion'],"Generado": str(row['evento_created'])[:16]}
                #r.hset(name="Semaforos", mapping=mapping)
                #ttl = 60*60 # 1 hora
                #r.expire(name="Semaforos", time=ttl)
                try:
                    bot.send_location(destinatario, latitude=row['evento_latitud'], longitude=row['evento_longitud'])
                except (IOError, Exception) as err:
                    logging.error(f"No se pudo enviar ubicación de evento: {err}.")
                resp = ''
    elif (not eventos or len(eventos)==0) and (not schedule):
        bot.send_message(destinatario, "No se encontraron eventos.")

def ping_semaforos(msg=None, df=None, schedule=False):
    global BOT_REPORTS
    if not msg:
        msg = BOT_REPORTS
    if schedule == False:
        bot.send_message(msg, "Conectando con semáforos:")
    print("Haciendo Ping a Semáforos:")
    if not equipos.empty:
        if df is not None:
            equipos['Estado'] = np.NaN
            equipos['Cantidad'] = 0
        raw_list = []
        offline = []
        online = []
        otro= []
        resp = ''
        notificar_off = ''
        notificar_on = ''

        def ping(host, idx):
            host = str(host)
            result = subprocess.run('ping -c1 -n '+host, shell=True).returncode
            raw_list.append(host+ ' '+ str(result))
            # Online
            if result == 0:
                online.append(host)
                if equipos.at[idx,'Estado'] == 0:
                    equipos.at[idx,'Cantidad'] = equipos.at[idx,'Cantidad']+1
                else:
                    equipos.at[idx,'Estado'] = 0
                    equipos.at[idx,'Cantidad'] = 1
            # Offline
            elif result == 1:
                result = subprocess.run('ping -c1 -n '+host, shell=True).returncode
                offline.append(host)
                if equipos.at[idx,'Estado'] == 1:
                    equipos.at[idx,'Cantidad'] = equipos.at[idx,'Cantidad']+1
                else:
                    equipos.at[idx,'Estado'] = 1
                    equipos.at[idx,'Cantidad'] = 1
            else:
                otro.append(host)

        num_threads = 78
        try:
            logging.info("Iniciando threads para ping")
            if schedule == False:
                bot.send_chat_action(msg, 'typing')  # show the bot "typing" (max. 5 secs)
            for idx, row in equipos.iterrows():
                if idx <= num_threads:
                    t = threading.Thread(target=ping, args=(row['IP_Controlador'],idx))
                    t.start()
            t.join()
        except (IOError, Exception) as err:
            logging.error(f"Error al iniciar trheads Ping: {err}.")
        
        if schedule == False:
            bot.send_chat_action(msg, 'typing')  # show the bot "typing" (max. 5 secs)
        time.sleep(5)
        for idx, row in equipos.iterrows():
            if idx <= num_threads:
                resp = resp+row['Cruce']+" : "+get_emoji(row['Estado'])+"\n"
                # Respuesta para comando "Semaforos"
                #resp = resp+row['Cruce']+" : "+str(row['Estado'])+"\n"
                # Listado semaforos offline
                if (row["Tipo_Dispositivo"]=="Semaforo") and (row['Estado'] == 1.0) and (row["Cantidad"] == 2)\
                    and (diff.seconds > 60*3):
                    notificar_off = notificar_off + row['Cruce']+" "+u'✖️'+"\n"
                diff = datetime.now() - init_time
                # Listado semáforos de vuelta online
                if (row["Tipo_Dispositivo"]=="Semaforo") and (row['Estado'] == 0.0) and (row["Cantidad"] == 1)\
                    and (diff.seconds > 120): # para evitar notificar al arranque del script
                    notificar_on = notificar_on + row['Cruce']+" "+u'✅'+"\n"

        bot.send_chat_action(msg, 'typing')
        time.sleep(5)
        if not schedule:
            bot.send_message(msg, resp)
            bot.send_message(msg, f"✅ En Línea: {len(online)}\n✖️ Offline: {len(offline)}\n") #Otro: {len(otro)}\n
        else:
            if notificar_off != '':
                notificar_off = to_bold("Semáforo(s) Fuera de Línea: ⚠️\n")+notificar_off
                bot.send_message(msg, notificar_off)
            if notificar_on != '':
                notificar_on = to_bold("Semáforo(s) de nuevo En Línea: \n")+notificar_on
                #bot.send_message(msg, notificar_on) ----------> Activar para volver a notificar en línea.
        print(raw_list)
        print(equipos)
        return equipos, resp, notificar_off, notificar_on
    else:
        #bot.send_message(message.chat.id, "Error:")
        print("*** ERROR *****")
        resp = ''
        notificar_off = ''
        notificar_on = ''
        return equipos, resp, notificar_off, notificar_on

###########################################
###########################################
# --------- SALUDO -------------- 

"""
def solo_autorizados(funcion):
    def funcion_b():
        if message.chat.username in AUTORIZADOS:
            funcion()
        else:
            CONOCIDOS.append(message.chat.id)
    return funcion()

@bot.message_handler(commands=['hola'])
@solo_autorizados()
def hola(message):
    bot.send_message(message.chat.id, "Hola")

"""

@bot.message_handler(commands=['start', 'help'])
def saludo(message):
    """Saludo o /start. Envía el teclado personalizado al usuario."""
    # Saludo
    name = message.chat.username
    global NOTIFICADOS
    global CONOCIDOS
    if (name in AUTORIZADOS):
        user = message.from_user
        msg = f"Hola {user.first_name}.\nBot del Centro Avanzado de Gestión de Tráfico de Asunción.\n"
        bot.send_message(message.chat.id, msg)

        # Teclado personalizado
        markup = types.ReplyKeyboardMarkup(row_width=2)
        # Botones
        btn_actual = types.KeyboardButton('Informes Actuales. ⏱')
        btn_dia = types.KeyboardButton('Informes del Día. 📅')
        btn_semaforos = types.KeyboardButton('Estado de Semaforos. 🚦')
        btn_eventos = types.KeyboardButton('Eventos Cerca de Semáforos. 📡')
        markup.add(btn_actual, btn_dia, btn_semaforos, btn_eventos)
        # Envio de mensajes y teclado.
        bot.send_message(message.chat.id, "Opciones:", reply_markup=markup)
    elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
        CONOCIDOS.append(name)
        bot.send_message(message.chat.id, f"No autorizado. {name}")
            
# --- Funciones para los Botones ---
# /resumen_ahora
@bot.message_handler(regexp='Informes Actuales.')
@bot.message_handler(commands=['resumen_ahora'])
def informes_actuales(message, estado=INFORMES_ACTUALES):
    name = message.chat.username
    global NOTIFICADOS
    global CONOCIDOS
    if (name in AUTORIZADOS):
        if estado:
            resumen(message, "Actual")
        else:
            bot.send_message(message.chat.id, "Servicio no disponible por el momento.")
    elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
        CONOCIDOS.append(name)
        bot.send_message(message.chat.id, f"No autorizado. {name}")

# /resumen_hoy
@bot.message_handler(regexp='Informes del Día.')
@bot.message_handler(commands=['resumen_hoy'])
def informes_dia(message, estado=INFORMES_DIA):
    name = message.chat.username
    global NOTIFICADOS
    global CONOCIDOS
    if (name in AUTORIZADOS):
        if estado:
            hoy, hoy_str, tz_hoy = tiempo()
            resumen(message, hoy_str)
        else:
            bot.send_message(message.chat.id, "Servicio no disponible por el momento.")   
    elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
        CONOCIDOS.append(name)
        bot.send_message(message.chat.id, f"No autorizado. {name}")

#/semaforos
RESP=''
notificar_off = ''
notificar_on = ''
@bot.message_handler(regexp='Estado de Semaforos.')
@bot.message_handler(commands=['semaforos'])
def estado_semaforos(message, estado=ESTADO_SEMAFOROS, schedule=False):
    name = message.chat.username
    global NOTIFICADOS
    global CONOCIDOS
    if (name in AUTORIZADOS):
        if message:
            msg = message.chat.id
        else:
            msg = BOT_REPORTS
        global RESP
        global notificar_off
        global notificar_on
        if estado:
            semaforos(message)
        else:
            global HACIENDO_PING
            if HACIENDO_PING is False:
                HACIENDO_PING = True
                if not schedule:
                    if len(RESP)>0:
                        bot.send_message(msg, to_bold("Último estado conocido:\n")+RESP[:4000])
                    else:
                        bot.send_message(msg, "Conectando con semáforos:")
                        equipos, resp, notificar_off, notificar_on = ping_semaforos(msg=msg, schedule=False)
                else:
                    equipos, resp, notificar_off, notificar_on = ping_semaforos(msg=msg, schedule=True)
                HACIENDO_PING = False
    elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
        CONOCIDOS.append(name)
        bot.send_message(message.chat.id, f"No autorizado. {name}")

#/eventos
@bot.message_handler(regexp='Eventos Cerca')
@bot.message_handler(commands=['eventos']) 
def eventos_cercanos(message, estado=EVENTOS_CERCANOS, schedule=False):
    if estado and schedule:
        get_eventos_cercanos(message, schedule=True)
    elif estado:
        name = message.chat.username
        global CONOCIDOS
        if (name in AUTORIZADOS):
            get_eventos_cercanos(message)
        elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
            CONOCIDOS.append(name)
            bot.send_message(message.chat.id, f"No autorizado. {name}")
    else:
        bot.send_message(message.chat.id, "Servicio no disponible por el momento.")   

@bot.message_handler(commands=['info'])
def info(message):
    name = message.chat.username
    global NOTIFICADOS
    global CONOCIDOS
    if (name in AUTORIZADOS):
        resp = ""

        diff = datetime.now() - init_time

        for i in NOTIFICADOS:
            resp = resp + i + "\n"
        bot.send_message(message.chat.id, f"NOTIFICADOS: {resp[:70]}")
        bot.send_message(message.chat.id, f"Cantidad: {len(NOTIFICADOS)}")
        bot.send_message(message.chat.id, f'Inicio: {init_time.strftime("%d/%m/%Y %H:%M")}')
        bot.send_message(message.chat.id, f'Tiempo activo: {diff}')
    elif (name not in AUTORIZADOS) and (name not in CONOCIDOS):
        CONOCIDOS.append(name)
        bot.send_message(message.chat.id, f"No autorizado. {name}")
    
@bot.message_handler(commands=['reset']) 
def reset(message):
    global NOTIFICADOS
    bot.send_message(message.chat.id, f"Reseteado NOTIFICADOS: {len(NOTIFICADOS)}")
    NOTIFICADOS.clear()
    bot.send_message(message.chat.id, f"Reseteado NOTIFICADOS: {len(NOTIFICADOS)}")

@bot.message_handler(commands=['hora']) 
def reset(message):
    bot.send_message(message.chat.id, datetime.now())

if __name__ == '__main__':
    equipos = get_equipos()

    def resumen_callable():
        """Resuelve problema de llamar al metodo [do] de schedule, con parametros."""
        resumen(destino=BOT_REPORTS, periodo="Actual", schedule=True)
    
    def eventos_callable():
        """llamar a eventos cercanos con argumentos."""
        get_eventos_cercanos(estado=EVENTOS_CERCANOS, schedule=True)

    def ping_callable():
        """llamar a estado de semaforos con argumentos."""
        ping_semaforos(schedule=True)

    def start_():
        bot.infinity_polling()

    # Llama a resumen_callable (envios programados de eventos)
    try:
        #schedule.every(10).minutes.do(resumen_callable)
        schedule.every(2).minutes.do(eventos_callable)
        #schedule.every(1).minutes.do(ping_callable)
    except(IOError, Exception) as err:
        print(f"Error: {err}.")

    def cron_():
        """Cron para tareas definidas con schedule."""
        while True:
            schedule.run_pending()
            time.sleep(2)

    # Thread para Inicio y generales.
    t_start = threading.Thread(target=start_)
    # Thread para Resumenes programados.
    t_cron_ = threading.Thread(target=cron_)
    # Arranque
    t_start.start()
    t_cron_.start()
