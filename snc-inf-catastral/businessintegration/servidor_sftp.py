import base64
import os
import pathlib
from datetime import datetime
import pysftp

import business as b


def establecer_conexion() -> pysftp.Connection:
    """
    Funcion para estabecer la conexion con el servidor sftp

    :return: (pysftp.Connection)
        Conexion con el servidor sftp
    """
    username = base64.b64decode(os.getenv('SFTP_USERNAME').encode()).decode()
    password = base64.b64decode(os.getenv('SFTP_PASSWORD').encode()).decode()
    conn = pysftp.Connection(
        os.getenv('SFTP_HOST'),
        username=username,
        password=password
    )
    return conn


def cargar_archivo(ruta_archivo_local: str, ruta_remota: str, archivo_remoto: str):
    """
    Funcion encargada se subir un archivo local al servidor SFTP

    :param (str) ruta_archivo_local:
        Ruta absoluta del archivo local a cargar

    :param (str) ruta_remota:
        Ruta absoluta donde se cargara el archivo

    :param (str) archivo_remoto:
        nombre del archivo

    :return: (None)
    """
    with establecer_conexion() as sftp:
        sftp.makedirs(ruta_remota)
        b.logging.info(f"Directorio remoto para -> {ruta_remota} creado")
        sftp.put(ruta_archivo_local, f"{ruta_remota}/{archivo_remoto}")
        b.logging.info(f"Fichero remoto -> {ruta_remota}/{archivo_remoto} cargado")


def construir_ruta_de_reporte(fecha: datetime, id_municipio: int) -> str:
    """
    Funcion que construye la ruta local donde se almcena el reporte generado

    :param datetime fecha:
        Fecha usada como parametro de busqueda del disparador en la base de datos, se usa para obtener el nombre del
        archivo XTF

    :param int id_municipio:
        Id del municipio al que corresponde el reporte

    :return: (str)
        Ruta del archivo local a subir al servidor SFTP
    """
    raiz_ruta = pathlib.Path(__name__).parent.absolute()
    reportes_ruta = os.path.join(raiz_ruta, 'reportes')
    ruta_reporte = os.path.join(reportes_ruta, f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}.xtf")
    return ruta_reporte


def descargar_remover_archivo(ruta_archivo_remoto: str, ruta_archivo_local: str):
    """
    Funcion para descargar archivo del servidor SFTP

    :param str ruta_archivo_remoto:
        Ruta donde se almacena el archivo localmente

    :param str ruta_archivo_local:
        Ruta del archivo remoto a descargar

    :return:
    """
    b.logging.info(f"Descargando archivo remoto -> {ruta_archivo_remoto}")
    with establecer_conexion() as sftp:
        sftp.get(ruta_archivo_remoto, ruta_archivo_local)
        sftp.remove(ruta_archivo_remoto)


def descargar_info_geografica(fecha: datetime, id_municipio: str, geojson_generados: list):
    """
    Funcion que descarga el archivo con la informacion geografica del servidor SFTP

    :param datetime fecha:
        Fecha usada como parametro de busqueda del disparador en la base de datos, se usa para obtener el nombre del
        archivo XTF
    :param id_municipio:
        Id del municipio al que corresponde el reporte
    :param geojson_generados:
        Listado de los geoJSON que se generaron y se encuentran en el FTP
    :return:
    """
    raiz_ruta = pathlib.Path(__name__).parent.absolute()
    ruta_informacion_geografica = os.path.join(raiz_ruta, 'reportes', f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}")

    ruta_archivo_remoto_base = f"{os.getenv('SFTP_REPORTS_PATH')}{id_municipio}/{fecha.year}/{id_municipio}_" \
                               f"{fecha.strftime('%Y-%m-%d')}"
    b.logging.info(f'ruta remota base  para info geo-> {ruta_archivo_remoto_base} generada')
    b.logging.info(f'Ruta local generada para info geo-> {ruta_informacion_geografica}')
    with establecer_conexion() as sftp:
        if 'Construccion' in geojson_generados:
            sftp.get(f'{ruta_archivo_remoto_base}_Construccion.json', f'{ruta_informacion_geografica}_Construccion.json')
        if 'Unidad' in geojson_generados:
            sftp.get(f'{ruta_archivo_remoto_base}_Unidad.json', f'{ruta_informacion_geografica}_Unidad.json')
        if 'Terreno' in geojson_generados:
            sftp.get(f'{ruta_archivo_remoto_base}_Terreno.json', f'{ruta_informacion_geografica}_Terreno.json')
        if 'CTM12' in geojson_generados:
            sftp.get(f'{ruta_archivo_remoto_base}_TERRENO_INFORMALIDAD.json', f'{ruta_informacion_geografica}_TERRENO_INFORMALIDAD.json')
        sftp.get(f'{ruta_archivo_remoto_base}_GeoDictionary.json', f'{ruta_informacion_geografica}_GeoDictionary.json')

        if os.getenv('ELIMINAR_TXT') == '1':
            if 'Construccion' in geojson_generados:
                sftp.remove(f'{ruta_archivo_remoto_base}_Construccion.json')
            if 'Unidad' in geojson_generados:
                sftp.remove(f'{ruta_archivo_remoto_base}_Unidad.json')
            if 'Terreno' in geojson_generados:
                sftp.remove(f'{ruta_archivo_remoto_base}_Terreno.json')
            if 'CTM12' in geojson_generados:
                sftp.remove(f'{ruta_archivo_remoto_base}_TERRENO_INFORMALIDAD.json')
            sftp.remove(f'{ruta_archivo_remoto_base}_GeoDictionary.json')

    return ruta_informacion_geografica


def subir_archivo_xtf(fecha: datetime, id_municipio: int) -> str:
    """
    Funcion encargada de subir el archivo XTF generado al servidor SFTP

    :param datetime fecha:
        Fecha usada como parametro de busqueda del disparador en la base de datos, se usa para obtener el nombre del
        archvibo XTF

    :param int id_municipio:
        Id del municipio al que corresponde el reporte

    :return: (str)
        Ruta del reporte cargado en el servidor remoto
    """
    ruta_archivo_local = construir_ruta_de_reporte(fecha, id_municipio)
    b.logging.info(f"Ruta local generada para el reporte XTF -> {ruta_archivo_local}")

    directorio_remoto = f"{os.getenv('SFTP_REPORTS_PATH')}{id_municipio}/{fecha.year}/"
    archivo_remoto = f"{id_municipio}_{fecha.strftime('%Y-%m-%d')}.xtf"
    cargar_archivo(ruta_archivo_local, directorio_remoto, archivo_remoto)
    os.remove(ruta_archivo_local)
    return f"{directorio_remoto}{archivo_remoto}"
