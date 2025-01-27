import json
import os
import pathlib
import time
from datetime import datetime

import pandas
import websocket
from websocket import WebSocketConnectionClosedException

import business as b
from alfanumerico import snc_extrac
from businessintegration import servidor_sftp
from database import disparadores_repository, archivos_generados_repository, reglas_validacion_repository
from database.ejecuciones_repository import EjecucionesRepository
from models.archivos_generados import ArchivosGenerados
from models.ejecuciones import Ejecuciones

FORMATO_FECHA = '%Y-%m-%d'


def aplicar_reglas_de_validacion(id_municipio: int, fecha: datetime, errores_generacion: list,
                                 resultado_extraccion_geografica: bool) -> (str, int):
    """
    Funcion que aplica las reglas de validacion que se encuentren activa en la base de datos

    :param (int) id_municipio:
        Codigo del municipio al que corresonde el reporte

    :param fecha:
        Fecha a la que corresponde el reporte

    :param (list) errores_generacion
        Lista con errores a nivel de generación

    :param bool resultado_extraccion_geografica
        Booleano que indica si la extracción geográfica fue satisfactoria

    :return: (tuple)
        Tupla que contiene la ruta de los logs en el servidor SFTP y la cantidad de registros con errores
    """
    # Extraer la reglas de validacion activas en la base de datos
    b.logging.info(f"Obteniendo reglas de validación de la información alfanumérica para id municipio -> "
                   f"{id_municipio}")
    reglas = reglas_validacion_repository.find_active_rules()

    # Variable donde se almacenaran los registros con errores
    logs_alfa = []

    # Iterar las reglas para aplciarlas
    for regla in reglas:
        inicio = time.time()
        # Consultar la informacion erronea en la base de datos
        logs_alfa += snc_extrac.extraer_informacion_erronea(regla.regla, id_municipio, regla.id,
                                                       regla.descripcion.replace('\n', ''))
        fin = time.time()
        b.logging.info(f"Id municipio -> {id_municipio}"
                       f"Tiempo ejecución consulta -> {str(fin - inicio)} segundos para regla -> {regla.descripcion}")

    # Aplicar contadores de errores consecutivos
    # b.logging.info(f"Inicio actualizacion de errores consecutivos para el id municipio -> {id_municipio}")
    inicio = time.time()
    # contadores = snc_extrac.aumentar_contadores_de_errores(logs, id_municipio)
    fin = time.time()
    # b.logging.info(f"Tiempo de ejecucion para aumento de contadores para el id municipio -> {id_municipio} -> "
                   #f"{str(fin - inicio)} segundos")

    raiz_ruta = pathlib.Path(__name__).parent.absolute()
    ruta_logs_errores_geograficos = None

    if resultado_extraccion_geografica:
        ruta_logs_errores_geograficos = os.path.join(raiz_ruta, 'reportes', f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}"
                                                                     f"_Log_Errores.json")
        servidor_sftp.descargar_remover_archivo(f"{os.getenv('SFTP_REPORTS_PATH')}{id_municipio}/{fecha.year}/"
                                                f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}_Log_Errores.json",
                                                ruta_logs_errores_geograficos)

        errores_geo_list = json.loads(open(ruta_logs_errores_geograficos, 'r').read())
        dataframe_errores_geo = pandas.DataFrame(errores_geo_list)
        if len(errores_geo_list) > 0:
            dataframe_errores_geo.columns = ['Numero predial', 'Departamento', 'Municipio', 'ID error', 'Error']
        os.remove(ruta_logs_errores_geograficos)

    dataframe_errores_gen = pandas.DataFrame(errores_generacion)
    if len(errores_generacion) > 0:
        dataframe_errores_gen.columns = ['Numero predial', 'Municipio', 'Departamento', 'Id de error',
                                         'Descripción de error']

    # Si no se encontraron registros erroneos no se genera el archivo xlsx
    if len(logs_alfa) > 0:
        dataframe_errores_alf = pandas.DataFrame(logs_alfa)
        dataframe_errores_alf.columns = ['Id predio', 'Numero predial', 'Municipio', 'Departamento', 'Id regla',
                                         'Regla']
        # dataframe_contadores = pandas.DataFrame(contadores)
        # if len(contadores) > 1:
        # dataframe_contadores.columns = ['Numero predial', 'Errores consecutivos']

        # dataframe_contadores.columns = ['Predio', 'Errores']

        b.logging.info(f"Inicio generación archivo excel con logs de errores para el id municipio -> {id_municipio}")
        inicio = time.time()

    nombre_archivo = f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}.xlsx"
    ruta_excel_local = os.path.join(os.getcwd(), 'reportes', nombre_archivo)
    ruta_excel_remota = f"{os.getenv('SFTP_REPORTS_PATH')}{id_municipio}/{fecha.year}/"

    if resultado_extraccion_geografica or len(logs_alfa) > 0 or len(errores_generacion) > 0:
        with pandas.ExcelWriter(ruta_excel_local) as writer:
            if len(logs_alfa) > 0:
                dataframe_errores_alf.to_excel(writer, sheet_name='Errores info alf', index=False)
            if resultado_extraccion_geografica:
                dataframe_errores_geo.to_excel(writer, sheet_name='Errores info geo', index=False)
            if len(errores_generacion) > 0:
                dataframe_errores_gen.to_excel(writer, sheet_name='Errores generación XTF', index=False)
            # dataframe_contadores.to_excel(writer, sheet_name='Errores consecutivos', index=False)

        fin = time.time()
        b.logging.info(f"Tiempo de generaración de excel para id municipio -> {id_municipio} -> {str(fin - inicio)} "
                       f"segundos")
        b.logging.info(f'Subiendo archivo remoto -> {ruta_excel_remota} al servidor SFTP para id municipio -> '
                    f'{id_municipio}')
        servidor_sftp.cargar_archivo(ruta_excel_local, ruta_excel_remota, nombre_archivo)
        os.remove(ruta_excel_local)
        return f"{ruta_excel_remota}{nombre_archivo}", len(logs_alfa)

    return None, 0

def generar_reporte_hilo(
        disparador,
        fecha: datetime,
        ejecucion: dict,
        ejecuciones_repository_obj: EjecucionesRepository,
        resultado_extraccion_geografica: dict
) -> bool:
    """
    Funcion sobre la cual se creara un hilo al momento de que el servidor geografico responda indicando que la
    informacion geografica ya fue generada

    :param disparador:
        Clase con los datos del disparador a ejecutar

    :param (datetime) fecha:
        Fecha a la que corresponde la ejecucion

    :param (dict) ejecucion:
        Diccionario con los datos de la ejecucion

    :param (EjecucionesRepository) ejecuciones_repository_obj:
        Objeto de la clase encargada de realizar operaciones sobre la base de datos

    :param (dict) resultado_extraccion_geografica
        Diccionario que contiene la respuesta obtenida del servidor geográfico

    :return: (bool)
        Valor booleano que indica si el proceso de generacion del reporte fue exitosa

    """
    ruta_informacion_geografica = None
    if resultado_extraccion_geografica['exito']:
        try:
            # Descargar el archivo con la informacion geografica del servidort SFTP
            ruta_informacion_geografica = servidor_sftp.descargar_info_geografica(fecha, disparador.id_municipio, resultado_extraccion_geografica['archivos'])
        except Exception as ex:
            b.logging.exception(f'Error al obtener la información geográfica para id municipio -> '
                                f'{disparador.id_municipio} Error -> {ex}')
            return False

    # Iniciar proceso de generacion del reporte XTF
    geojson_generados = []
    geojson_generados = None
    if 'archivos' in resultado_extraccion_geografica:
        geojson_generados = resultado_extraccion_geografica['archivos']
    errores_xtf = snc_extrac.generar_xtf(str(disparador.id_municipio), ruta_informacion_geografica, fecha,
                                         resultado_extraccion_geografica['exito'], geojson_generados)
    b.logging.info(f"Proceso de generación de reporte XTF para el id municipio -> {disparador.id_municipio} "
                   f"finalizado")

    if resultado_extraccion_geografica['exito']:
        # Eliminar archivo locales con la informacion geografica
        if 'Construccion' in resultado_extraccion_geografica['archivos']:
            os.remove(os.path.join(f'{ruta_informacion_geografica}_Construccion.json'))
        if 'Unidad' in resultado_extraccion_geografica['archivos']:
            os.remove(os.path.join(f'{ruta_informacion_geografica}_Unidad.json'))
        if 'Terreno' in resultado_extraccion_geografica['archivos']:
            os.remove(os.path.join(f'{ruta_informacion_geografica}_Terreno.json'))
        if 'CTM12' in resultado_extraccion_geografica['archivos']:
            os.remove(os.path.join(f'{ruta_informacion_geografica}_TERRENO_INFORMALIDAD.json'))
        os.remove(os.path.join(f'{ruta_informacion_geografica}_GeoDictionary.json'))

    # Cargar el archivo XTF generado al servidor FTP
    ruta_remota_reporte = servidor_sftp.subir_archivo_xtf(fecha, disparador.id_municipio)

    # Persisitir los datos del archivo generado en la BD
    archivo = ArchivosGenerados(id_ejecucion=ejecucion['id'], ruta=ruta_remota_reporte, eliminado=False)
    archivos_generados_repository.insert(archivo)

    # Generar log de errores
    ruta_log_errores, cantidad_errores = aplicar_reglas_de_validacion(disparador.id_municipio, fecha, errores_xtf,
                                                                      resultado_extraccion_geografica['exito'])
    
    # Actualizar el estado de la generacion de reporte en BD
    ejecuciones_repository_obj.update_success(ruta_log_errores, cantidad_errores)

    b.logging.info(f"Generación del reporte para id municipio -> {disparador.id_municipio} finalizada")

    return True


def extraer_informacion_catastral_business() -> None:
    """
    Funcion que ejecuta la logica para generacion del reporte XTF

    :return: (None)
    """
    # Guardar fecha en caso de que las ejecuciones se extiendan hasta mas de media noche
    fecha = datetime.strptime(datetime.now().strftime(FORMATO_FECHA), FORMATO_FECHA)
    b.logging.info(f"Iniciando ejecución reporte catastral para -> {fecha.strftime('%Y-%m-%d')}")

    # Obtener lista de reportes a ejecutar
    disparadores_a_ejecutar = disparadores_repository.find_by_date_to_execute(fecha)
    municipios = []
    for i in disparadores_a_ejecutar:
        municipios.append(i.id_municipio)

    b.logging.info(f"Lista de municipios obtenidos de la BD para generar reporte -> {municipios}")
    del municipios

    if len(disparadores_a_ejecutar) > 0:
        b.logging.info(f"Creando conexion con servidor geográfico")
        try:
            ws = websocket.create_connection(f"{os.getenv('WS_PROTOCOL')}://{os.getenv('WS_HOST')}:"
                                             f"{os.getenv('WS_PORT')}")
        except (ConnectionRefusedError, ConnectionResetError):
            b.logging.error(f"Error el establecer conexión con servidor geográfico")
            return

        b.logging.info(f"Conexión con servidor geográfico establecida")
        for disparador in disparadores_a_ejecutar:
            # Persistir la informacion de ejecucion inicial del reporte
            ejecucion = Ejecuciones(
                id_disparador=disparador.id,
                id_estado=1,
                fecha_ejecucion=datetime.now(),
                exitoso=False,
                registros_con_errores=0
            )

            ejecuciones_repository_obj = EjecucionesRepository()
            ejecucion = ejecuciones_repository_obj.insert(ejecucion)
            b.logging.info(f"Ejecución insertada en la DB para id municipio -> {disparador.id_municipio}")

            msg = {'id_municipio': disparador.id_municipio, 'fecha': fecha.strftime(FORMATO_FECHA)}
            # Notifica al servidor el código del municipio del que necesita información geográfica
            b.logging.info(f"Enviando id municipio -> {disparador.id_municipio} al servidor geográfico")
            ws.send(json.dumps(msg))
            b.logging.info(f"Esperando respuesta del servidor geográfico para id municipio -> "
                           f"{disparador.id_municipio}")
            try:
                # A la espera de que el servidor gis notifique la creación del fichero de texto en el FTP
                response = json.loads(ws.recv())
                b.logging.info(f"Respuesta obtenida del servidor geográfico -> {json.dumps(response)} para id municipio"
                               f"-> {disparador.id_municipio}")
                generar_reporte_hilo(disparador, fecha, ejecucion, ejecuciones_repository_obj, response)
            except (ConnectionResetError, WebSocketConnectionClosedException):
                b.logging.error(f'Conexión con servidor geográfico perdida id municipio actual -> '
                               f'{disparador.id_municipio}')
                return
            except Exception as ex:
                b.logging.exception(f"Exception -> {ex}")
