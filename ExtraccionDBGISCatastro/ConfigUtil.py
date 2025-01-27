# -*- coding: utf-8 -*-
import os,datetime
import arcpy
from dotenv import load_dotenv
from shutil import rmtree

load_dotenv()
"""
Clase para obtener los parametros de ejecución necesarios en la extracción geografica, estos parametoos se encuentran en .env
"""
class Config(object):
        '''
        Keys
        '''
        SFTP_HOST = 'SFTP_HOST'
        SFTP_USERNAME = 'SFTP_USERNAME'
        SFTP_PASSWORD = 'SFTP_PASSWORD'

        WS_PROTOCOL = 'WS_PROTOCOL'
        WS_HOST = 'WS_HOST'
        WS_PORT = 'WS_PORT'

        SFTP_CONECTIONFILES_PATH = 'SFTP_CONECTIONFILES_PATH'
        SFTP_REPORTS_PATH = 'SFTP_REPORTS_PATH'
        SFTP_CTM12_PATH = 'SFTP_CTM12_PATH'
        SFTP_GCS_GRS_1980_PATH = 'SFTP_GCS_GRS_1980_PATH'

        BASE_PATH = 'BASE_PATH'
        OGR2OGR_PATH = 'OGR2OGR_PATH'

        DB_HOST = 'DB_HOST'
        DB_PORT = 'DB_PORT'
        DB_NAME = 'DB_NAME'
        DB_USER = 'DB_USER'
        DB_PW = 'DB_PW'

        def __init__(self):
                      
            self.config = {}
            self.load_app_config()

        """
        Se crea a ruta de trabajo actual para toda la operación de extracción 
        """

        def generate_directory(self):   
                
                out_folder_path = self.get_value(Config.BASE_PATH)
                time = datetime.datetime.now()
                out_name = time.strftime("%Y") + time.strftime("%m") + time.strftime("%d")
                # Se crea una carpeta donde se alojaran los archivos necesarios en la operación
                # Se valida la existencia del directorio
                using_folder= os.path.exists(out_folder_path+'\\'+out_name)
                if  using_folder == True:
                    rmtree(out_folder_path+'\\'+out_name)
                arcpy.CreateFolder_management(out_folder_path, out_name)
                 # Entorno de trabajo donde se procesaran los archivos generados en export e import
                work_path = out_folder_path+'\\'+out_name
                return work_path

        def load_app_config(self):
            keys  = [Config.SFTP_HOST,Config.SFTP_USERNAME,Config.SFTP_PASSWORD,Config.WS_PROTOCOL,Config.WS_HOST,Config.WS_PORT,Config.SFTP_CONECTIONFILES_PATH
                    ,Config.SFTP_REPORTS_PATH,Config.SFTP_CTM12_PATH,Config.SFTP_GCS_GRS_1980_PATH,Config.BASE_PATH,Config.OGR2OGR_PATH,Config.DB_HOST,Config.DB_PORT
                    ,Config.DB_NAME,Config.DB_USER,Config.DB_PW] 
            for item in keys:
                self.config[item] = os.getenv(item)
              
        def get_value(self, key):
            return self.config[key]        

