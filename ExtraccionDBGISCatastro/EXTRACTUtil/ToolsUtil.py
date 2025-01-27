# -*- coding: utf-8 -*-
import arcpy
import json
import subprocess
import pygeoj
import gc
from ConfigUtil import Config

"""
Clase funcional del codigo cuyas funciones tienen toda la logica seguida por el codigo
"""
class ToolsProcesamiento(object):
    
    
    def __init__(self,logger):
        self.logger = logger 
        self.config = Config()
        self.OGR2OGR_PATH = self.config.get_value('OGR2OGR_PATH')
    '''
    Valida Si el municipio tiene un sistema de coordenas igual a CTM12
    '''
    def CTM12_validation(self,SDE_path,in_gdb_path,out_gdb_path):
        FcCTM12 = "U_TERRENO_CTM12" 
        arcpy.env.workspace = SDE_path
        if arcpy.Exists(FcCTM12):
            print("-----------Municipio con Sistema de Coordenadas igual a CTM12------------------")
            self.logger.debug('"-----------Municipio con Sistema de Coordenadas igual a CTM12------------------"')
            OutGdb_UCTM12= in_gdb_path+ '\\URBANO_CTM12'
            OutGdb_RCTM12=in_gdb_path+ '\\RURAL_CTM12'
            CTM12 = True
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_CONSTRUCCION_CTM12', OutGdb_UCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_CONSTRUCCION_CTM12', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_UNIDAD_CTM12', OutGdb_UCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_UNIDAD_CTM12', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_TERRENO_CTM12', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_TERRENO_CTM12', OutGdb_UCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_CONSTRUCCION_INFORMAL', OutGdb_UCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_CONSTRUCCION_INFORMAL', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_UNIDAD_INFORMAL', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_UNIDAD_INFORMAL', OutGdb_UCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_TERRENO_INFORMAL', OutGdb_RCTM12)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_TERRENO_INFORMAL', OutGdb_UCTM12)

            ListDelete_R = ['R_CONSTRUCCION_CTM12','R_UNIDAD_CTM12','R_TERRENO_CTM12',
                            'R_CONSTRUCCION_INFORMAL','R_UNIDAD_INFORMAL','R_TERRENO_INFORMAL']
            ListDelete_U =['U_CONSTRUCCION_CTM12','U_UNIDAD_CTM12','U_TERRENO_CTM12',
                        'U_CONSTRUCCION_INFORMAL','U_UNIDAD_INFORMAL','U_TERRENO_INFORMAL']
            field = ["CODIGO_ANTERIOR"]

            self.delete_fields(ListDelete_R,field,OutGdb_RCTM12)
            self.delete_fields(ListDelete_U,field,OutGdb_UCTM12)

            arcpy.Merge_management([in_gdb_path + '\\R_CONSTRUCCION_CTM12', in_gdb_path + '\\R_CONSTRUCCION_INFORMAL'], out_gdb_path + '\\R_CONSTRUCCION_CTM12PPM')
            arcpy.Merge_management([in_gdb_path + '\\U_CONSTRUCCION_CTM12', in_gdb_path + '\\U_CONSTRUCCION_INFORMAL'], out_gdb_path+ '\\U_CONSTRUCCION_CTM12PPM')
            arcpy.Merge_management([in_gdb_path + '\\U_UNIDAD_CTM12', in_gdb_path + '\\U_UNIDAD_INFORMAL'], out_gdb_path+ '\\U_UNIDAD_CTM12PPM')
            arcpy.Merge_management([in_gdb_path + '\\R_UNIDAD_CTM12', in_gdb_path + '\\R_UNIDAD_INFORMAL'], out_gdb_path + '\\R_UNIDAD_CTM12PPM')
            arcpy.Merge_management([in_gdb_path + '\\R_TERRENO_CTM12', in_gdb_path + '\\R_TERRENO_INFORMAL'], out_gdb_path + '\\R_TERRENO_CTM12PPM')
            arcpy.Merge_management([in_gdb_path + '\\U_TERRENO_CTM12', in_gdb_path + '\\U_TERRENO_INFORMAL'], out_gdb_path + '\\U_TERRENO_CTM12PPM')
            arcpy.Merge_management([out_gdb_path + '\\R_CONSTRUCCION_CTM12PPM', out_gdb_path + '\\U_CONSTRUCCION_CTM12PPM'], out_gdb_path + '\\CONSTRUCCION_CTM12NM')
            arcpy.Merge_management([out_gdb_path + '\\R_TERRENO_CTM12PPM', out_gdb_path + '\\U_TERRENO_CTM12PPM'], out_gdb_path + '\\TERRENO_CTM12NM')
            arcpy.Merge_management([out_gdb_path + '\\U_UNIDAD_CTM12PPM', out_gdb_path + '\\R_UNIDAD_CTM12PPM'], out_gdb_path + '\\UNIDAD_CTM12NM')
        else:
            print("-----------Municipio con Sistema de Coordenadas distinto a CTM12------------------")   
            self.logger.debug('"-----------Municipio con Sistema de Coordenadas distinto a CTM12------------------"')
            CTM12 = False
        return CTM12
    """
    Se extraen y se valida si los Feature class de un municipio estan vacias con el fin de evitar errores de ejecución
    NOTA #1: Proyectar un feature class a CTM12 Municipios cuyo feature class esta vacio
    NOTA #2: Si ambas feature class Terreno estan vacias se descarta la ejecucion del municipio
    NOTA #3: estos siempre deben de existir en la gdb 
    """
    def Void_feature_validate(self,in_gdb,SDE_path):
        OutGdb_U = in_gdb + '\\URBANO'
        OutGdb_R = in_gdb + '\\RURAL'
        try:
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_CONSTRUCCION', OutGdb_U)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_TERRENO', OutGdb_U)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\U_UNIDAD', OutGdb_U)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_CONSTRUCCION', OutGdb_R)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_TERRENO', OutGdb_R)
            arcpy.conversion.FeatureClassToGeodatabase(SDE_path+'\\R_UNIDAD', OutGdb_R)  
        except Exception as ex:
            print (ex) 
            self.logger.error("No se logro extraer los Feature class de la gdb corporativa")
            raise NameError("No se logro extraer los Feature class de la gdb corporativa")
            
        CountT_U = arcpy.GetCount_management(in_gdb+"\\U_TERRENO")
        CountT_R = arcpy.GetCount_management(in_gdb+"\\R_TERRENO")
        CountU_C = arcpy.GetCount_management(in_gdb+"\\U_CONSTRUCCION")
        CountR_C = arcpy.GetCount_management(in_gdb+"\\R_CONSTRUCCION")
        CountU_U = arcpy.GetCount_management(in_gdb+"\\U_UNIDAD")
        CountR_U = arcpy.GetCount_management(in_gdb+"\\R_UNIDAD")
        ListExis = []
       
        if (str (CountT_U)=='0') and (str (CountT_R)=='0'):
                print('Tablas de terreno vacias se descartara el municipio')
                self.logger.error('Tablas de terreno vacias, se descartara el municipio')
                raise NameError("Error Tablas de Terreno Vacia")
        
        if str (CountU_C) != '0':
            ListExis.append('U_CONSTRUCCION')
        if str (CountT_R) != '0':
            ListExis.append('R_TERRENO')
        if str (CountT_U) != '0':
            ListExis.append('U_TERRENO')
        if str (CountR_C) != '0':
            ListExis.append('R_CONSTRUCCION')
        if str  (CountU_U) != '0':
            ListExis.append('U_UNIDAD')
        if str (CountR_U) != '0':
            ListExis.append('R_UNIDAD')
        return ListExis

    """
    Projecta a CTM12 los municipios con sistema de coordenadas distintos es este mismo
    """
    def Project (self,Feature_list,in_gdb,out_gdb,CTM12):
        lists_Out = []
        items = ['TERRENO','CONSTRUCCION','UNIDAD']
        self.logger.debug('Se inicia la proyección para las siguientes Feature class: '+ str (Feature_list))
        try:
            for item in Feature_list:
                in_feature =  in_gdb + '\\'+item
                out_feature = out_gdb + '\\'+item +'_CTM12PM'
                arcpy.Project_management(in_feature, out_feature, CTM12)
        except Exception as e:
            print (e)
            self.logger.error('Ocurrio un error durante la proyección a CTM12 del municipio:  '+ str (e))
            raise Exception (e)
        for item in items:
            if ('U_'+item in Feature_list) and ('R_'+item in Feature_list):
                lists_Out.append(item)
                arcpy.Merge_management([out_gdb + '\\U_'+item+'_CTM12PM', out_gdb + '\\R_'+item+'_CTM12PM'], out_gdb+'\\'+item+'_PCTM12')
            if ('U_'+item in Feature_list) and ('R_'+item not in Feature_list):
                lists_Out.append(item)
                arcpy.CopyFeatures_management( out_gdb + '\\U_'+item +'_CTM12PM', out_gdb+'\\'+item+'_PCTM12')
            if ('U_'+item not in Feature_list) and ('R_'+item  in Feature_list):
                lists_Out.append(item)
                arcpy.CopyFeatures_management( out_gdb + '\\R_'+item +'_CTM12PM', out_gdb+'\\'+item+'_PCTM12')
        self.logger.debug('Proyección Terminada con exito')
        return lists_Out

    def delete_fields(self,features,fields,path):
        for itemR in features:   
                arcpy.DeleteField_management(path+'\\'+itemR,fields)
    

    def Editor_Util(self,Name,fieldNames,gdb):
        VoidList = []
        ##################################-----------------EDITOR-----------------#############################################
        self.logger.debug('Se inician Transformaciones para el preprocesamiento del municipio para el Feature class: '+ Name)
        F_Count = len(fieldNames) 
        path = gdb + '\\'+ Name +'_CTM12'
        if Name == 'UNIDAD':

                EditorU = arcpy.da.Editor(gdb)
                EditorU.startEditing(False, False)  
                EditorU.startOperation()

        if Name == 'CONSTRUCCION':

                EditorC = arcpy.da.Editor(gdb)

                EditorC.startEditing(False, False)  
                EditorC.startOperation()

        with arcpy.da.UpdateCursor(path, fieldNames) as cursor:  
            for row in cursor:  
                rowU = row 
                for field in range(F_Count):
                    if rowU[2] == 'No Convencional' or rowU[2] == 'NO CONVENCIONAL':  
                        rowU[2] = 'NO_CONVENCIONAL'
                    elif rowU[1]== None or rowU[1] == ' ':
                        VoidList.append(rowU[0])
                        rowU[1] = "000000000000000000000000000000"
                    elif rowU[0] == None:  
                        rowU[0] = "000000000000000000000000000000"   
                    elif rowU[3] == None:
                        rowU[3]= "000"
                    elif rowU[2] == None:
                        rowU[2]= "000"    
                    elif rowU[3] == "\n"  or  rowU[3] == "\t":
                        rowU[3] = "-"
                                   
                cursor.updateRow(rowU)
        del cursor
        del rowU
        if Name == 'UNIDAD':
                EditorU.stopOperation()
                EditorU.stopEditing(True)
        if Name == 'CONSTRUCCION':
                EditorC.stopOperation()
                EditorC.stopEditing(True)
        self.logger.debug('Transformaciones terminadas con exito para: '+Name)
        return VoidList
    '''
    Hace las transformaciones necesarias para una correcta operación de la data en el orquestador
    '''
    def Soporte__transformacion(self,name,Result,gdb):
            if name == 'UNIDAD':
                
                fieldNames = ["CONSTRUCCION_CODIGO","TERRENO_CODIGO","TIPO_CONSTRUCCION","IDENTIFICADOR"]
            if name == 'CONSTRUCCION':
                
                fieldNames = ["CODIGO","TERRENO_CODIGO","TIPO_CONSTRUCCION","IDENTIFICADOR"]
            
            if ( name in Result) and (name == 'UNIDAD'):
   
                self.logger.debug('"-----------editor UNIDAD------------------"')
                ListU = self.Editor_Util(name,fieldNames,gdb)
                self.logger.debug('"-----------editor Termino con exito para: UNIDAD------------------"')
                return ListU
            elif( name in Result) and name == 'CONSTRUCCION':

                self.logger.debug('"-----------editor  CONSTRUCCION------------------"')

                ListC = self.Editor_Util(name,fieldNames,gdb)
                self.logger.debug('"-----------editor Termino con exito para: CONSTRUCCION------------------"')
                return ListC 
            else:
                print('No exite el feature class '+name)
                self.logger.info('No exite el feature class  '+name+' No se procede con las transformaciones para este Feature class')
                void = []
                return void

    def drop_fields(self,Feature,Fields,gdb):
         Feature_path = gdb + '\\'+ Feature +'_CTM12'
         arcpy.DeleteField_management(Feature_path,Fields)
   
    def FeatureClassToShapefile(self,Feature,gdb,ruta):
        Feature_path = gdb + '\\'+ Feature +'_CTM12'
        arcpy.FeatureClassToShapefile_conversion (Feature_path, ruta)
   
    def Check_geo_csv(self,feature_name,ruta,Result):
        if feature_name not in Result:
            return None
        path_csv = ruta+'\\'+feature_name+'_E.csv'
        file_shp = ruta+'\\'+feature_name+'_CTM12.shp'
        arcpy.CheckGeometry_management(file_shp,ruta+'\\'+feature_name+'_E.csv')
        return path_csv

    '''
    Valida la informalidad de los predios y genera una serie de relaciones para mostrarlas en el xtf (Siempre y cuando estas existan)
    '''
    def validar_informales(self,FeatureName,gdb,ruta,fecha,Mun):
                self.logger.debug('-------------------Se inicia la validación los predios informales y sus relaciones con terreno---------------------------')
                R_PathFormal=gdb + '\\R_'+FeatureName+'_CTM12'
                R_PathInformal=gdb + '\\R_'+FeatureName+'_INFORMAL'
                R_out = gdb + '\\RURAL_'+FeatureName+'_INTERSECT'
                U_PathFormal=gdb + '\\U_'+FeatureName+'_CTM12'
                U_PathInformal=gdb + '\\U_'+FeatureName+'_INFORMAL'
                U_out = gdb + '\\URBANO_'+FeatureName+'_INTERSECT'
                out = gdb+ '\\MERGE_'+FeatureName+'_INTERSECT'
                arcpy.Intersect_analysis([R_PathFormal, R_PathInformal],R_out,"ALL", "0.002 METERS", "INPUT")
                arcpy.Intersect_analysis([U_PathFormal, U_PathInformal],U_out,"ALL", "0.002 METERS", "INPUT")
                arcpy.Merge_management([R_out, U_out], out)
                
                drop = ["FID_R_"+FeatureName+'_CTM12','VEREDA_CODIGO','USUARIO_LOG','FECHA_LOG',"GLOBALID_SNC","CODIGO_MUNICIPIO",
                        "FID_R_"+FeatureName+'_INFORMAL','USUARIO_LOG_1','FECHA_LOG_1',"GLOBALID_SNC_1","CODIGO_MUNICIPIO_1",
                        "FID_U_"+FeatureName+'_CTM12','MANZANA_CODIGO', "FID_U_"+FeatureName+'_INFORMAL']
                arcpy.DeleteField_management (out, drop)
                arcpy.TableToTable_conversion (out, ruta,FeatureName+'_RELATIONS.csv')
                file = open(ruta+'\\'+FeatureName+'_RELATIONS.csv', 'r')
                dic = {}
                for line in file:
                    x = line.split(';')
                    x[2].replace('\n','')
                    if x[1] in dic.keys():

                        dic[x[1]].append(x[2])
                    else:
                        dic[x[1]] = [x[2]]
                file.close()
                objson = json.dumps(dic,indent=6)
                ruta_reporte =ruta+'\\'+Mun+"_"+ fecha +'_TERRENO_INFORMALIDAD.json'
                temp_file =  open(ruta_reporte, 'w+') 
                self.logger.debug('-------------------Validacion de Informales terminada con exito---------------------------')
                for item in objson:
                    temp_file.write(item)
                return ruta_reporte
    '''
    Hace un check de la geometria y hace un drop de los poligonos que no cumplen con una topología permitida, estos se envian en el logg espacial
    '''
    def Check_geometry(self,feature_name,ruta,path_csv):
                if path_csv == None:
                    return None
                file_shp = ruta+'\\'+feature_name+'_CTM12.shp'
                file_error = open(path_csv, 'r')
                fid = []
                problem = []
                for col in file_error:
                    s = col.split(';')
                    if  s[2]!="-1": 
                        if s[2]!="FEATURE_ID":
                            fid.append(int (s[2]))  
                        problem.append(feature_name +' con error '+s[3].replace('\n',''))
                problem.remove(str (problem[0])) 
                file_error.close()

                ListCod = []
                if len (fid) == 1:
                    fid.append(-1)
                if len (fid) == 0:
                    fid.append(-1)
                    fid.append(-2)

                Unique_tuple = tuple(fid)
                expression = """FID IN {} """.format(Unique_tuple)
                with arcpy.da.UpdateCursor(file_shp, ["FID","CODIGO"],expression) as cursor:
                    for row in cursor:
                        ListCod.append(row[1])
                        cursor.deleteRow()
                del cursor
                
                ListRet = []
                ListRet.append(ListCod)
                ListRet.append(problem)

                return ListRet
    '''
    Hace uso de la herramiento OGR2OGR para hacer la conversión a GeoJson de los shapes que contienen CONSTRUCCION, TERRENO Y UNIDAD
    '''
    def OGR2OGR(self,Name,fecha,ruta,Mun,Result):
        if Name not in Result:
            return None
        self.logger.debug('Se incia la conversión a geojson por parte de OGR2OGR para: '+ Name)
        Feature = ruta + '\\'+Name+"_CTM12.shp"
        json = ruta + '\\GeoJson_' + Mun +"_"+ fecha +"_"+Name+'.json'
        command = self.OGR2OGR_PATH +' -skipfailures -f GeoJSON ' +json+' '+ Feature+' -makevalid'
        subprocess.check_call(command)
        self.logger.debug('Se termina con exito la conversion con OGR2OGR para : '+ Name)
        return json
    '''
    Genera un diccionario con los datos necesarios para acceder por pygeoj a los geojson
    '''
    def GeoDict(self,JsonPath,Count,ItemName):
        if JsonPath == None:
            void = {}
            return void
        Temp_dict = {}
        Result_dict = {}
        self.logger.info('Se inicia la creación de diccionario json para acceder a los datos geograficos '+ ItemName )
        Geo = pygeoj.load(filepath=(JsonPath))
        for value in range (Count):
                Item = Geo.get_feature(int(value))
                Item_P =Item.properties
                Val_COD = Item_P['CODIGO']
                val_Ident=Item_P['IDENTIFICA']
                val_tipo= Item_P['TIPO_CONST'] 
                if Val_COD == None:
                        Val_COD = '0'
                if val_Ident == None:
                        val_Ident = '0'
                if val_tipo == None:
                        val_tipo = '0'  
                Temp_dict[Val_COD+'-'+val_tipo+'-'+val_Ident] = value

        Result_dict[ItemName] = Temp_dict  
        gc.collect()
        del Geo 
        del value
        del Temp_dict
        del Val_COD
        del val_tipo
        del  val_Ident
        del Item_P
        del Item
        self.logger.info('Diccionario Creado con exito '+ ItemName)
        return Result_dict

    def GeoDictTerreno(self,JsonPath,Count):
        Temp_dict = {}
        Terr_dict = {}
        self.logger.info('Se inicia la creación de diccionario json para acceder a los datos geograficos de Terrreno')
        Geo_Terr = pygeoj.load(filepath=(JsonPath))
        for value in range (Count):
            Item_T = Geo_Terr.get_feature(int(value))
            Item_terr =Item_T.properties
            Val_terreno = Item_terr['CODIGO']
            Temp_dict[Val_terreno] = value
        
        Terr_dict['Terreno'] =  Temp_dict
        del Temp_dict
        del Geo_Terr
        del Item_T 
        del Val_terreno
        del Item_terr
        del value
        self.logger.info('Diccionario Creado con exito de Terreno')
        return Terr_dict

        
    def Get_Count(self,ruta,FeatureName,Result):
        if FeatureName not in Result:
            return None
        FeaturePath = ruta +'\\'+ FeatureName +'_CTM12.shp'
        Count = arcpy.GetCount_management(FeaturePath)
        Count = int (str (Count))
        return Count