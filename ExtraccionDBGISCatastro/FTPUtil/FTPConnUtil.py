# -*- coding: utf-8 -*-
import pysftp
import base64, os
from ConfigUtil import Config

class FTPLogicaUtil(object):

    def __init__(self,logger):
        self.logger = logger
        self.config = Config()    
        self.FTP_HOST = self.config.get_value('SFTP_HOST')
        self.FTP_FilesCon = self.config.get_value('SFTP_CONECTIONFILES_PATH')
        self.FTP_FilesCon = self.config.get_value('SFTP_REPORTS_PATH')
        self.FTP_FilesCTM12 = self.config.get_value('SFTP_CTM12_PATH')
        self.FTP_FilesGRS1980 = self.config.get_value('SFTP_GCS_GRS_1980_PATH')

        self.FTP_USER = base64.b64decode(self.config.get_value('SFTP_USERNAME').encode()).decode()
        self.FTP_PWD =  base64.b64decode(self.config.get_value('SFTP_PASSWORD').encode()).decode()


    def establecer_conexion(self):
        username = self.FTP_USER
        password = self.FTP_PWD
        self.logger.debug('---------------------------------Se intenta establecer conexión con servidor FTP---------------------------------')
        conn = pysftp.Connection(
            host= self.FTP_HOST,
            username=username,
            password=password
        )
        self.logger.debug('---------------------------------Conexión establecida---------------------------------')
        return conn

    def archivo_proyeccion(self,ruta):
        with self.establecer_conexion() as sftp:
            ruta_local = ruta +'\\CTM12.prj' 
            directorio_remoto = self.FTP_FilesCTM12
            ruta_archivo_remoto = directorio_remoto + '/CTM12.prj'
            self.logger.info('############################### Se inicia la conexión al servidor FTP para extracción de .prj CTM12 ###############################')
            try:
                sftp.get(ruta_archivo_remoto, ruta_local)
            except Exception as ex:
                self.logger.error('Ocurrio un error en la extraccion de archivo CTM12: '+ str (ex))
                print(ex)
            sftp.close()  
            self.logger.info('############################### Se obtuvo con exito .prj CTM12 ###############################')
            
        return ruta_local 


    def archivo_InfoMunicipiosDepartamentos(self,ruta):
        with self.establecer_conexion() as sftp:
            ruta_local = ruta +'\\InfoMunicipiosDepartamentos.csv' 
            directorio_remoto = self.FTP_FilesCTM12
            ruta_archivo_remoto = directorio_remoto + '/InfoMunicipiosDepartamentos.csv'
            self.logger.info('############################### Se inicia la conexión al servidor FTP para extracción de InfoMunicipiosDepartamentos.csv ###############################')
            try:
                sftp.get(ruta_archivo_remoto, ruta_local)
            except Exception as ex:
                self.logger.error('Ocurrio un error en la extraccion de archivo InfoMunicipiosDepartamento.csv del FTP : '+ str (ex))
                print(ex)
            self.logger.info('############################### Se obtuvo con exito  InfoMunicipiosDepartamentos.csv ###############################')
            sftp.close()  
            
        return ruta_local 

    def logica(self,id_municipio,fecha,ruta,Result,CTM12):
        print('id_municipio: ' + str(id_municipio) + '_fecha: ' + fecha)
        raiz_ruta = ruta
        
        ruta_reporte_Construccion = os.path.join(raiz_ruta,'GeoJson_' + str(
            id_municipio) + '_' + fecha + '_Construccion.json')
        ruta_reporte_Terreno = os.path.join(raiz_ruta,'GeoJson_' + str(
            id_municipio) + '_' + fecha + '_Terreno.json')
        ruta_reporte_Unidad = os.path.join(raiz_ruta,'GeoJson_' +  str(
            id_municipio) + '_' + fecha + '_Unidad.json') 
        ruta_reporte_Log = os.path.join(raiz_ruta,str (
            id_municipio)+ '_' + fecha + '_Log_Errores.json')  
        ruta_reporte_Dict = os.path.join(raiz_ruta,'GeoJson_Dictionary_'+str (
            id_municipio)+ '_' + fecha + '.json')
        ruta_reporte_CSV = os.path.join(raiz_ruta,str (
            id_municipio)+ '_' + fecha + '_TERRENO_INFORMALIDAD.json')

        with self.establecer_conexion() as sftp:
            directorio_remoto = os.getenv('SFTP_REPORTS_PATH') + \
                str(id_municipio) + '/' + fecha.split('-')[0]
            ruta_archivo_remoto_Construccion = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_Construccion.json'
            ruta_archivo_remoto_Terreno = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_Terreno.json'
            ruta_archivo_remoto_Unidad = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_Unidad.json'
            ruta_archivo_remoto_LogErrores = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_Log_Errores.json'    
            ruta_archivo_remoto_Dict = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_GeoDictionary.json'      
            ruta_archivo_remoto_CSV = ruta_archivo_remoto_Dict = directorio_remoto + '/' + \
                str(id_municipio) + '_' + fecha + '_TERRENO_INFORMALIDAD.json'    
            sftp.makedirs(directorio_remoto)
            self.logger.info('--------------------------Se inicia la carga de los json para el orquestador-----------------------------')
            try:
                if 'CONSTRUCCION' in Result:
                    sftp.put(ruta_reporte_Construccion, ruta_archivo_remoto_Construccion)
                if 'UNIDAD' in Result:
                    sftp.put(ruta_reporte_Unidad, ruta_archivo_remoto_Unidad)    
                if CTM12 == True:
                    sftp.put(ruta_reporte_CSV, ruta_archivo_remoto_CSV)  
                sftp.put(ruta_reporte_Terreno, ruta_archivo_remoto_Terreno)
                sftp.put(ruta_reporte_Log, ruta_archivo_remoto_LogErrores)
                sftp.put(ruta_reporte_Dict, ruta_archivo_remoto_Dict)
            except Exception as ex:
                self.logger.error('Ocurrio un error en la carga de la json para el orquestador : '+ str (ex))
                print(ex)
            self.logger.info('--------------------------Archivos Json cargados con exito en el FTP-----------------------------')
            sftp.close()
            

            return ruta_archivo_remoto_LogErrores