# -*- coding: utf-8 -*-
import csv,json
import logging, datetime
from FTPUtil.FTPConnUtil import FTPLogicaUtil
from ConfigUtil import Config
"""
Clase que genera un log de ejecución interna, lo puede consultar en:  C:\\workspace\\log.txt
"""
class logger(object):
    
    def __init__(self):
        self.start = datetime.datetime.now()
        self.config = Config()
        self.PathLogs= self.config.get_value('BASE_PATH')
    def config_logger(self):
        ##### La ruta prederterminada para el log de ejecución es la misma raiz de workspace
        logFormat = '%(asctime)s (%(name)s) %(levelname)s (%(module)s) - %(message)s'
        logfileReconcile = self.PathLogs +"\\log.txt"
        className = type(self).__name__
        logging.basicConfig(level=logging.DEBUG, format=logFormat, filename=logfileReconcile, filemode='a')
        logging.getLogger("requests").setLevel(logging.INFO)
        self.logger = logging.getLogger(":"+className)
        self.logger.debug("******************************************************************************************************************")

"""
Clase que genera un log espacial cuyos datos contenidos son errores espaciales que debierón omitirse para una generación correcta del xtf
"""

class Logg_Espacial(object):

    def __init__(self,logger):
        self.logger = logger 
        self.FTP = FTPLogicaUtil(self.logger)
    def LoggEspacial(self,ruta,VoidListsC,VoidListsU,ErroList,SlideList,Mun,Fecha):
        self.logger.debug('Ejecutando logs espaciales')
        Datacsv = self.FTP.archivo_InfoMunicipiosDepartamentos(ruta)
        dict_mun = {}
        dict_dep = {}
        inp = open(Datacsv, mode='r') 
        reader = csv.reader(inp)
        dict_dep= {rows[0]:rows[2] for rows in reader}
        inp = open(Datacsv, mode='r') 
        reader2 = csv.reader(inp)
        dict_mun = {rows[0]:rows[1] for rows in reader2}
        
        listaSlideError = []
        VoidLists = VoidListsC + VoidListsU

        for element in VoidListsC:
                listaSlideError.append(("ERROR:  EXISTE UNA CONSTRUCCION SIN TERRENO CON CODIGO: "+ element))
        for element in VoidListsU:
                if element == None:
                    element = 'null'
                listaSlideError.append(("ERROR:  EXISTE UNA UNIDAD SIN TERRENO CON CODIGO: "+ element))
       
        listaSlideError =  listaSlideError + SlideList
        ListaE = VoidLists + ErroList
        
        obj = [[ListaE[i],dict_dep[Mun],dict_mun[Mun],"142",listaSlideError[i]] for i in range(len(ListaE))]
    
        objson = json.dumps(obj,indent=6) 
        ruta_reporte_Log = ruta + '\\' + Mun +"_"+ Fecha +"_Log_Errores"+'.json'
        temp_file =  open(ruta_reporte_Log, 'w+') 
        for item in objson:
            temp_file.write(item)
        self.logger.debug('logs espaciales terminados con exito')
        del obj   
        del temp_file
        return ruta_reporte_Log

