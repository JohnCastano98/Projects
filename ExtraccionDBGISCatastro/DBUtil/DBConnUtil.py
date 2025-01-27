# -*- coding: utf-8 -*-
import arcpy
import base64 ,time, os
import cx_Oracle
from ConfigUtil import Config

""" 
Clase para administrar conexiones a Oracle y Crear conexiones .SDE para base de datos corporativa 
"""


class OracleManager(object):


    def __init__(self,logger):

        self.config = Config()
        self.DB_HOST = self.config.get_value('DB_HOST')
        self.DB_PORT = self.config.get_value('DB_PORT')
        self.DB_NAME = self.config.get_value('DB_NAME') 
        self.DB_USER = base64.b64decode(self.config.get_value('DB_USER').encode()).decode()
        self.DB_PWD =  base64.b64decode(self.config.get_value('DB_PW').encode()).decode()
        self.logger = logger 

    def get_connection(self):
        db_conn = None
        
        self.logger.debug("**Se inicia Conexión a base de datos alfanumerica:")
        self.logger.debug(self.DB_NAME)

        NUMERO_MAXIMO_INTENTOS = 10
        numeroIntentos = 0

        #Intenta conectarse a la base de datos. En caso de error realiza reintentos (Maximo determinado por NUMERO_MAXIMO_INTENTOS )
        lastError = ""
        while numeroIntentos <= NUMERO_MAXIMO_INTENTOS:
            try:
                numeroIntentos  += 1
                ## crea el dsn. Parametros :  ip, port, sid or database name
                dsn_tns = cx_Oracle.makedsn(self.DB_HOST, self.DB_PORT, service_name=self.DB_NAME)
                db_conn = cx_Oracle.connect(self.DB_USER, self.DB_PWD, dsn_tns)
                break
            #except error_perm, e:
            except Exception as e:
                errorMsg = "[ "+ str(e).strip() + " ]"
                self.logger.error(errorMsg)
                lastError = errorMsg
                
                time.sleep(numeroIntentos) #Cantidad de segundos
        
        if db_conn == None:
            raise Exception(errorMsg)    
        
        return db_conn

    def obtenerParametrosConexion(self,schema):
        db_conn = self.get_connection()
        try:
            self.logger.debug("** obtenerParametrosConexion schema: "+schema)
            sql = ''
            if schema == 'SNC_MUNICIPIOS':
                sql = "select valor_caracter from parametro  where nombre = 'INSTANCIA_SNC_MUNICIPIOS' "
            else:
                sql = "select conexion_gdb from MUNICIPIO_COMPLEMENTO where esquema_gdb = '"+schema+"' "
            
            self.logger.debug("** definiendo sql ")    

            db_conn.begin()
            cursor = db_conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchone()
            
            if results == None:
                errorMsg = " No se pudo consultar la tabla de parametros de conexion a la instancia de base de datos geografica del esquema : " + schema
                self.logger.error(errorMsg)
                raise Exception(errorMsg) 
            
            serviceName = results[0]
            #self.logger.debug("** serviceName: "+serviceName)
            return serviceName
        except Exception as e:
            errorMsg = "[ "+ str(e).strip() + " ]"
            raise Exception(errorMsg)
        finally:
            db_conn.close()


    def crearConexionSDE(self,schema,connFolder, versionName):
        try:
            self.logger.debug("antes de obtener parametros")
            serviceName = self.obtenerParametrosConexion(schema)
            self.logger.debug("despues de obtener parametros")
            #self.logger.debug("serviceName:"+serviceName)
            
            ##TODO leer de algún parámetro
            password = schema
            
            serverName  = "ORA_SERVER"
            databaseName = ""
            if serviceName == 'sde:oracle11g:indra200':
                serviceName = 'sde:oracle11g:indra200'
            else:
                serviceName = serviceName +'pru'
            authType = "DATABASE_AUTH"
            saveUserInfo = "SAVE_USERNAME"
            saveVersionInfo = "SAVE_VERSION"
            fileName = schema+"_"+versionName+".sde"
            dbConnPath = connFolder+"\\"+fileName
            self.logger.debug("serviceName:"+serviceName)       
            if os.path.exists(dbConnPath): 
                os.remove(dbConnPath)
            self.logger.debug("Inicia Crea Conn SDE : "+dbConnPath)
            arcpy.CreateArcSDEConnectionFile_management (connFolder, fileName, serverName, 
                serviceName, databaseName, authType, schema, password, saveUserInfo, versionName, saveVersionInfo)
            self.logger.debug("Fin Crea Conn SDE")
            return dbConnPath
        except Exception as e:
            print(e)
            errorMsg = "[ "+ str(e) + " ]"
            self.logger.error("Error en la creacion de: Conn SDE")
            raise Exception(errorMsg)