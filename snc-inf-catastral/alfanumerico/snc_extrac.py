import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import cx_Oracle
from sqlalchemy.exc import DatabaseError

import business as b
import database
import geografico.gis_extrac as gis_extrac
from alfanumerico import util
import uuid

# Diccionarios que contiene los uuid de cada registro
predios_uuid = {}
predios_uuid_num_predial = {}
construcciones_uuid = {}
caracteristicas_cons_uuid = {}
fuentes_uuid = {}
derechos_uuid = {}
tramites_uuid = {}
interesados_uuid = {}
interesados_clave = {}  # Relaciona la clave intermedia con el id interesado
interesados_normal = {}  # Relaciona el id interesado con el id interesado normlizado
agrup_inter_uuid = {}
matrices_uuid = {}
datos_ph_uuid = {}

# Listado de clases que han sido procesadas o deben ser omitidas
persona_predio_propiedad_procesados = []
construcciones_omitidas = []
unidades_omitidas = []
derechos_predio_id_procesados = []
predios_omitidos = []
interesados_procesados = []

MODELO_APLICACION_LADM_RIC_CONSTRUCCION = 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Construccion'
SEGUNDOS = 'segundos'

def limpiar_gobales():
    global predios_uuid
    global predios_uuid_num_predial
    global construcciones_uuid
    global caracteristicas_cons_uuid
    global fuentes_uuid
    global derechos_uuid
    global tramites_uuid
    global interesados_uuid
    global interesados_clave
    global interesados_normal
    global agrup_inter_uuid
    global matrices_uuid
    global datos_ph_uuid
    global persona_predio_propiedad_procesados
    global construcciones_omitidas
    global unidades_omitidas
    global derechos_predio_id_procesados
    global predios_omitidos
    global interesados_procesados

    predios_uuid = {}
    predios_uuid_num_predial = {}
    construcciones_uuid = {}
    caracteristicas_cons_uuid = {}
    fuentes_uuid = {}
    derechos_uuid = {}
    tramites_uuid = {}
    interesados_uuid = {}
    interesados_clave = {}
    interesados_normal = {}
    agrup_inter_uuid = {}
    matrices_uuid = {}
    datos_ph_uuid = {}
    persona_predio_propiedad_procesados = []
    construcciones_omitidas = []
    unidades_omitidas = []
    derechos_predio_id_procesados = []
    predios_omitidos = []
    interesados_procesados = []

def aumentar_contadores_de_errores(logs: list, id_municipio: int) -> list:
    """
    Funcion encargada de actualizar y obtener los contadores de errores consecutivos presentados

    :param (list) logs:
        Lista de predios con errores

    :param id_municipio:
        Id del municipio al que corresponde el reporte

    :return: (list)
        Lista de predios que presentan errores consecutivos en la generacion del reporte
    """
    predios_con_error = []
    for log in logs:
        database.session.execute(
            'CALL SNC_GENERALES.AUMENTAR_CONTADOR_DE_ERRORES(:predio)', params={'predio': log[0]})
        database.session.commit()
        predios_con_error.append(log[0])

    predios_con_error_str = str(tuple(predios_con_error))
    if len(predios_con_error) == 1:
        predios_con_error_str[len(predios_con_error) - 2] = ''
    query = """
            DELETE SNC_GENERALES.ERRORES_CONSECUTIVOS
            WHERE
            """
    if len(logs) > 0:
        query += """
                    ID_PREDIO NOT IN %s
                    AND
                 """ % predios_con_error_str
    query += """
                ID_PREDIO IN (
                    SELECT
                        ID
                    FROM PREDIO
                    WHERE
                        MUNICIPIO_CODIGO = :municipio
            )
       """

    with database.engine.connect() as conn:
        conn.execute(query, {'municipio': id_municipio})

    query = """
        SELECT
            p.NUMERO_PREDIAL,
            ec.ERRORES
        FROM SNC_GENERALES.ERRORES_CONSECUTIVOS ec
            INNER JOIN PREDIO p ON EC.ID_PREDIO = p.ID
        WHERE
            p.MUNICIPIO_CODIGO  = :municipio
    """
    with database.engine.connect() as conn:
        return conn.execute(query, {'municipio': id_municipio}).fetchall()


def extraer_informacion_erronea(query: str, municipio_codigo: int, id_regla: int, regla: str):
    """
    Funcion que ejecuta los querys necesarios para extraer la informacion erronea de la base de datos

    :param str query:
        Query nativo a ejecutar en la base de datos con los parametros municipio_codigo y regla

    :param int municipio_codigo:
        Codigo del municipio al que corresponde el reporte

    :param id_regla:
        ID de la regla de validacion a aplicar

    :param regla:
        Descripcion en lenguaje natural de la regla a aplicar

    :return: (list)
        Lista de registros encontrados en la base de datos
    """
    b.logging.info(
        f'Ejecutando consulta para generar log de errores ->\n{query}')
    with database.engine.connect() as conn:
        return conn.execute(query, municipio_codigo=municipio_codigo, id_regla=id_regla, regla=regla).fetchall()


def aumentar_contadores_de_errores(logs: list, id_municipio: int) -> list:
    """
    Funcion encargada de actualizar y obtener los contadores de errores consecutivos presentados

    :param (list) logs:
        Lista de predios con errores

    :param id_municipio:
        Id del municipio al que corresponde el reporte

    :return: (list)
        Lista de predios que presentan errores consecutivos en la generacion del reporte
    """
    predios_con_error = []
    for log in logs:
        database.session.execute(
            'CALL SNC_GENERALES.AUMENTAR_CONTADOR_DE_ERRORES(:predio)', params={'predio': log[0]})
        database.session.commit()
        predios_con_error.append(log[0])

    b.logging.info(
        f"Eliminando contadores no vigentes para id municipio -> {id_municipio}")
    query = """
            DELETE SNC_GENERALES.ERRORES_CONSECUTIVOS
            WHERE
                VIGENTE = 0
                AND
                ID_PREDIO IN (
                    SELECT
                        ID
                    FROM PREDIO
                    WHERE
                        MUNICIPIO_CODIGO = :municipio
                )
        """

    with database.engine.connect() as conn:
        conn.execute(query, {'municipio': id_municipio})
    b.logging.info(
        f"Ejecutando consulta de errores consecutivos para id municipio -> {id_municipio}")
    query = """
        SELECT
            p.NUMERO_PREDIAL,
            ec.ERRORES
        FROM SNC_GENERALES.ERRORES_CONSECUTIVOS ec
            INNER JOIN PREDIO p ON EC.ID_PREDIO = p.ID
        WHERE
            p.MUNICIPIO_CODIGO  = :municipio
    """
    result = []
    with database.engine.connect() as conn:
        result += conn.execute(query, {'municipio': id_municipio}).fetchall()
    b.logging.info(
        f"Actualizando contadores para id municipio -> {id_municipio} como no vigentes")
    query = """
             UPDATE SNC_GENERALES.ERRORES_CONSECUTIVOS
                SET VIGENTE = 0
             WHERE
                ID_PREDIO IN (
                    SELECT
                        ID
                    FROM PREDIO
                    WHERE
                        MUNICIPIO_CODIGO = :municipio
                )
            """
    with database.engine.connect() as conn:
        conn.execute(query, {'municipio': id_municipio})
    return result


def extraer_informacion_erronea(query: str, municipio_codigo: int, id_regla: int, regla: str):
    """
    Funcion que ejecuta los querys necesarios para extraer la informacion erronea de la base de datos

    :param str query:
        Query nativo a ejecutar en la base de datos con los parametros municipio_codigo y regla

    :param int municipio_codigo:
        Codigo del municipio al que corresponde el reporte

    :param id_regla:
        ID de la regla de validacion a aplicar

    :param regla:
        Descripcion en lenguaje natural de la regla a aplicar

    :return: (list)
        Lista de registros encontrados en la base de datos
    """
    query_log = query.replace('\n', ' ')
    b.logging.info(f'Ejecutando consulta para generar log de errores para id municipio -> {municipio_codigo} '
                   f'->\n{query_log}')
    try:
        with database.engine.connect() as conn:
            return conn.execute(query, municipio_codigo=municipio_codigo, id_regla=id_regla, regla=regla).fetchall()
    except DatabaseError as ex:
        b.logging.error(
            f'Error aplicando la regla de validación -> {query_log} -> {str(ex)}')
        return []


def extraer_data_alfanumerica(query, municipio_codigo):
    """
    Consulta la data alfanumerica y la inicializa en una lista

    :param str query:
        Nombre del fichero .sql que contiene la consulta

    :param str municipio_codigo:
        Código del municipio

    return: (list)
    """
    b.logging.info(
        f'Ejecutando consulta: {query} para id municipio -> {municipio_codigo}')
    with open(os.path.join(os.getcwd(), 'queries', query), 'r') as myfile:
        data = myfile.read()

    with database.engine.connect() as conn:
        if os.getenv('ENV') == 'TEST':
            where_sql = ''
            if query != 'predio.sql':
                where_sql += ' AND p.id IN (1'
                for predio_uuid in predios_uuid:
                    where_sql += f', {predio_uuid} '
                where_sql += ')'
                data = data.format(in_clause=where_sql)
                return conn.execute(data, municipio_codigo=municipio_codigo).fetchall()
            elif os.getenv('FAST_QUERY') == '1':
                data = data.format(rownum=' AND ROWNUM < 1000')
        else:
            if query != 'predio.sql':
                data = data.format(in_clause='')
            else:
                data = data.format(rownum='')

        return conn.execute(data, municipio_codigo=municipio_codigo).fetchall()


def agregar_uuid_a_diccionario(element_tree, tag_name, uuid):
    """
        Agrega elementos al diccionario de uuid que indica el tag_name

    Args:
        element_tree (ElemetTree): Árbol de elementos xml
        tag_name (str): Nombre de la etiqueta
        uuid (str): uuid generado
    """
    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Predio' in tag_name:
        predios_uuid[element_tree.find('id').text] = uuid
        predios_uuid_num_predial[element_tree.find(
            'Numero_Predial').text] = uuid
        # element_tree.remove(element_tree.find('id'))

    if MODELO_APLICACION_LADM_RIC_CONSTRUCCION in tag_name:

        construcciones_uuid[element_tree.find('id').text] = uuid
        # element_tree.remove(element_tree.find('id'))

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_CaracteristicasUnidadConstruccion' in tag_name:
        caracteristicas_cons_uuid[element_tree.find('id').text] = uuid
        # element_tree.remove(element_tree.find('id'))

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_FuenteAdministrativa' in tag_name:
        fuentes_uuid[element_tree.find('id').text] = uuid
        element_tree.remove(element_tree.find('id'))

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Derecho' in tag_name:
        derechos_uuid[element_tree.find('predio_id').text] = uuid

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Interesado' in tag_name:
        interesados_uuid[element_tree.find('id').text] = uuid
        # element_tree.remove(element_tree.find('id'))

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_AgrupacionInteresados' in tag_name:
        if element_tree.find('predio_id').text not in agrup_inter_uuid:
            agrup_inter_uuid[element_tree.find('predio_id').text] = uuid

    if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_DatosPHCondominio' in tag_name:
        if element_tree.find('predio_id').text not in datos_ph_uuid:
            datos_ph_uuid[element_tree.find('predio_id').text] = uuid


def generar_xtf(municipio_codigo: str, ruta_archivo: str, fecha: datetime, exito_extraccion_geografica: bool, geojson_generados: list):
    """
    Ejecuta la consulta a la base de datos alfanumérica relacionada al municipio,
    integra con la data geografica y genera un fichero .xtf en el dirctorio raíz

    :param str municipio_codigo:
        Código del municipio

    :param str ruta_archivo:
        Ruta local del archivo txt donde se encuentra la informacion geografica

    :param str fecha:
        Fecha en la que inicia la generación del reporte

    :param bool exito_extraccion_geografica
        Booleano que indica si la extracción geográfica fue satisfactoria
    """
    limpiar_gobales()
    
    errores = []
    total_inicio = time.time()

    # Cargue de información geografica
    coordenadas_terreno = {}
    coordenadas_construccion = {}
    coordenadas_unidad = {}
    geo_indices = {}
    predio_informalidad = {}

    if exito_extraccion_geografica:
        try:
            if 'Terreno' in geojson_generados:
                inicio = time.time()
                coordenadas_terreno = gis_extrac.cargar_geojson(
                    f'{ruta_archivo}_Terreno.json')
                fin = time.time()
                b.logging.info(
                    f'Tiempo de ejecución extrayendo datos para GeoJSON Terreno: {str(fin - inicio)} {SEGUNDOS} para id municipio -> '
                    f'{municipio_codigo}')
        except Exception as error:
            b.logging.exception(
                f'Ha ocurrido un error al intentar cargar el GeoJSON Terreno -> {error}')

        try:
            if 'Construccion' in geojson_generados:
                inicio = time.time()
                coordenadas_construccion = gis_extrac.cargar_geojson(
                    f'{ruta_archivo}_Construccion.json')
                fin = time.time()
                b.logging.info(
                    f'Tiempo de ejecución extrayendo datos para GeoJSON Construccion: {str(fin - inicio)} {SEGUNDOS} para id municipio -> '
                    f'{municipio_codigo}')
        except Exception as error:
            b.logging.exception(
                f'Ha ocurrido un error al intentar cargar el GeoJSON Construccion -> {error}')

        try:
            if 'Unidad' in geojson_generados:
                inicio = time.time()
                coordenadas_unidad = gis_extrac.cargar_geojson(
                    f'{ruta_archivo}_Unidad.json')
                fin = time.time()
                b.logging.info(
                    f'Tiempo de ejecución extrayendo datos para GeoJSON Unidad: {str(fin - inicio)} {SEGUNDOS} para id municipio -> '
                    f'{municipio_codigo}')
        except Exception as error:
            b.logging.exception(
                f'Ha ocurrido un error al intentar cargar el GeoJSON Unidad -> {error}')

        try:
            inicio = time.time()
            geo_indices = json.load(open(f'{ruta_archivo}_GeoDictionary.json'))
            fin = time.time()
            b.logging.info(
                f'Tiempo de ejecución extrayendo diccionario de índices: {str(fin - inicio)} {SEGUNDOS}'
                f'{municipio_codigo}')
        except Exception as error:
            b.logging.exception(
                f'Ha ocurrido un error al intentar cargar el diccionario de índices -> {error}')

        try:
            if 'CTM12' in geojson_generados:
                inicio = time.time()
                with open(f'{ruta_archivo}_TERRENO_INFORMALIDAD.json') as ctm12:
                    predio_informalidad = json.load(ctm12)
                fin = time.time()
                b.logging.info(
                    f'Tiempo de ejecución extrayendo JSON de informalidad: {str(fin - inicio)} {SEGUNDOS}')
        except Exception as error:
            b.logging.exception(
                f'Ha ocurrido un error al intentar cargar el JSON de informalidad -> {error}')

    try:
        # Crea el fichero .xtf en el directorio de reportes
        output = util.crear_fichero_salida(municipio_codigo, fecha, False)

        # Listado de relaciones predio - construccion que deben materializarse en la tabla
        # ueBaunit, contiene objeto de la forma -> {id del predio : TID de construccion}
        ue_baunit_queue = []
        col_rrr_fuente_queue = []
        predio_tramitecatastral_queue = []
        predio_copropiedad_queue = {}
        grupo_interesados_queue = {}
        caracteristicas_cons_queue = []
        col_unidadfuente_queue = []

        for query in util.QUERIES:
            municipio_omitido = False

            # Consulta de datos alfanuméricos
            inicio = time.time()
            rows = extraer_data_alfanumerica(query, municipio_codigo)
            fin = time.time()
            b.logging.info(
                f'Tiempo de ejecución de la consulta {query}: {str(fin - inicio)} {SEGUNDOS} para id municipio -> '
                f'{municipio_codigo}')

            if query == util.QUERIES[0] and len(rows) == 0:
                # Omite la ejecución la consulta de predio no retorna datos
                b.logging.info(
                    f'El municipio {municipio_codigo} se omite debido a que no cuenta con información reportable')
                municipio_omitido = True
                errores.append(
                    ['', '', '', '107',
                                 f'No existen datos reportables para el muncipio {municipio_codigo}'])

                output.close()
                output = util.crear_fichero_salida(
                    municipio_codigo, fecha, True)
                break

            for row in rows:

                content = row[0]
                root_tag = content.split('>')
                parser = ET.XMLParser(encoding='uft-8')
                xtf_fragment = ET.ElementTree(
                    ET.fromstring(content, parser=parser))
                root = xtf_fragment.getroot()

                temp_uuid = str(uuid.uuid4())
                root.set('TID', temp_uuid)

                # Omisión de interesados duplicados
                if ('Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Interesado' in root_tag[0]
                        and root.find('id').text in interesados_procesados):
                    continue

                agregar_uuid_a_diccionario(root, root_tag[0], temp_uuid)

                if ('Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Predio' not in root_tag[0]
                        and root.find('predio_id').text in predios_omitidos):
                    continue

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Predio' in root_tag[0]:
                    # Validacion fecha Vigencia_Actualizacion_Catastral
                    if root.find('Vigencia_Actualizacion_Catastral').text == '-01-01':
                        # Esto pasa cuando la función SNC_CONSERVACION.FNC_ANIOULTIMA_ACTUALIZACION
                        # no retorna una fecha por lo que el predio debe ser omitido
                        predios_omitidos.append(root.find('id').text)
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '110', f'No existe un una año de última actualización para este predio'])
                        continue

                    # Agrega a la cola de la relacion predio_copropiedad los predios que sean de tipo
                    # Propiedad horizontal, Condominio o Parques cemeterios
                    if root.find('Numero_Predial').text[21] in ['7', '8', '9']:
                        matriz = root.find(
                            'Numero_Predial').text[0:22] + '00000000'

                        if matriz != root.find('Numero_Predial'):
                            if matriz in predio_copropiedad_queue.keys():
                                predio_copropiedad_queue[matriz].append([
                                    root.find('id').text, root.find('Coeficiente').text])
                            else:
                                predio_copropiedad_queue[matriz] = [[
                                    root.find('id').text, root.find('Coeficiente').text]]
                        else:
                            matrices_uuid[matriz] = temp_uuid

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('id'))
                    root.remove(root.find('Coeficiente'))

                if ('Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_TramiteCatastral' not in root_tag[0]
                        and 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_DatosPHCondominio' not in root_tag[0]):
                    util.agregar_tag_local_id(root, str(uuid.uuid4()))

                if MODELO_APLICACION_LADM_RIC_CONSTRUCCION in root_tag[0]:
                    key = root.find('Numero_Predial').text + '-' + root.find(
                        'Tipo_Construccion').text.upper() + '-' + root.find('Unidad_Predial').text

                    existe_geometria = util.agregar_tag_geometria(
                        root, geo_indices, coordenadas_construccion, key, 'Construccion')
                    if not existe_geometria:
                        construcciones_omitidas.append(root.find('id').text)
                        construccion_cod = root.find('Numero_Predial').text
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '102', f'No se ha encontrado una geometría valida asociada a la construcción con código: {construccion_cod}'])
                        continue

                    ue_baunit_queue.append(
                        {
                            'predio': root.find('predio_id').text,
                            'ue': root.get('TID')
                        }
                    )

                    # Valida si el año de construcción es válido
                    if not util.valida_anio_construccion(root):
                        tid = root.attrib['TID']
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '103',
                             f'La construcción : {tid} no cuenta con un Anio_Construccion válido, se ha cambiado a vacío'])

                    # Elimina caracteres de control en la observación
                    if root.find('Observaciones').text != None:
                        root.find('Observaciones').text = root.find('Observaciones').text.replace(
                            '\n', ' ')
                        root.find('Observaciones').text = root.find('Observaciones').text.replace(
                            '\t', ' ')
                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('predio_id'))
                    root.remove(root.find('Numero_Predial'))
                    root.remove(root.find('Unidad_Predial'))
                    root.remove(root.find('id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_FuenteAdministrativa' in root_tag[0]:
                    ente_emisor = root.find('Ente_Emisor').text
                    if ente_emisor != None:
                        ente_emisor_trim = ente_emisor.replace('\t', ' ')
                        root.find('Ente_Emisor').text = ente_emisor_trim
                    col_unidadfuente_queue.append(
                        {
                            'unidad': root.find('predio_id').text,
                            'fuente_administrativa': root.get('TID')
                        }
                    )

                    root.remove(root.find('predio_id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Derecho' in root_tag[0]:
                    try:
                        util.agregar_tag_interesado_unidad(root, root.find('predio_id').text,  root.find('persona_id').text, predios_uuid,
                                                           interesados_uuid, interesados_normal, agrup_inter_uuid, grupo_interesados_queue)
                        derechos_predio_id_procesados.append(
                            root.find('predio_id').text)
                    except Exception:
                        # Si cae en la excepción quiere decir que intento relacionar un interesado que tenía errores en su
                        # tipo de documento y no fue cargado en el diccionario, por lo tanto se omite toda la etiqueta
                        continue

                    col_rrr_fuente_queue.append(
                        {
                            'id': root.find('id').text,
                            'derecho': root.get('TID')
                        }
                    )

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('id'))
                    root.remove(root.find('predio_id'))
                    root.remove(root.find('persona_id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_TramiteCatastral' in root_tag[0]:
                    if root.find('Clasificacion_Mutacion').text == None:
                        # Omite los trámites que tengan vacía su Clasificacion_Mutacion
                        resolucion = root.find('Numero_Resolucion').text
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '107', f'El trámite de resolución {resolucion} no cuenta con una Clasificacion_Mutacion por lo que se omite'])
                        continue
                    predio_tramitecatastral_queue.append(
                        {
                            'predio': root.find('predio_id').text,
                            'tramite': root.get('TID')
                        }
                    )
                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('predio_id'))
                    root.remove(root.find('Numero_Predial'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_UnidadConstruccion' in root_tag[0]:
                    # Si la construcción fue omitida la unidad no es escrita
                    if root.find('id').text in construcciones_omitidas:
                        unidades_omitidas.append(root.find('id').text)
                        continue

                    key = root.find('Numero_Predial').text + '-' + root.find(
                        'Tipo_Construccion').text.upper() + '-' + root.find('Unidad_Predial').text
                    existe_geometria = util.agregar_tag_geometria(
                        root, geo_indices, coordenadas_unidad, key, 'Unidad')

                    if not existe_geometria:
                        terreno_cod = root.find('Numero_Predial').text
                        unidades_omitidas.append(root.find('id').text)
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '104', f'No se ha encontrado una geometría valida asociada a la unidad con código: {terreno_cod}'])
                        continue

                    # Agrega la referencia de CaracterisitcasUnidadConstruccion y Construccion
                    # en la etiqueta UnidadConstruccion
                    util.agregar_relaciones_unidad_construccion(root, root.find(
                        'id').text, construcciones_uuid, caracteristicas_cons_uuid)

                    ue_baunit_queue.append(
                        {
                            'predio': root.find('predio_id').text,
                            'ue': root.get('TID')
                        }
                    )

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('id'))
                    root.remove(root.find('predio_id'))
                    root.remove(root.find('Numero_Predial'))
                    root.remove(root.find('Unidad_Predial'))
                    root.remove(root.find('Tipo_Construccion'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Terreno' in root_tag[0]:
                    existe_geometria = util.agregar_tag_geometria(
                        root, geo_indices, coordenadas_terreno, root.find('Numero_Predial').text, 'Terreno')
                    if not existe_geometria:
                        construccion_cod = root.find('Numero_Predial').text
                        errores.append(
                            [root.find('Numero_Predial').text, '', '', '105', f'No se ha encontrado una geometría valida asociada al terreno con código: {construccion_cod}'])
                        continue

                    ue_baunit_queue.append(
                        {
                            'predio': root.find('predio_id').text,
                            'ue': root.get('TID')
                        }
                    )

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('Numero_Predial'))
                    root.remove(root.find('predio_id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_CaracteristicasUnidadConstruccion' in root_tag[0]:
                    if root.find('id').text in unidades_omitidas:
                        continue
                    else:
                        # Valida si el año de construcción es válido
                        if not util.valida_anio_construccion(root):
                            tid = root.attrib['TID']
                            errores.append(
                                [root.find('Numero_Predial').text, '', '', '106',
                                 f'La CaracteristicaUnidadConstruccion: {tid} no cuenta con un Anio_Construccion válido, se ha cambiado a vacío'])
                        caracteristicas_cons_queue.append(root)

                        # Elimina atributos que no son parte del reporte
                        root.remove(root.find('Numero_Predial'))
                        root.remove(root.find('predio_id'))
                        continue

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_Interesado' in root_tag[0]:

                    # Normalización del interesado
                    clave = root.find('Tipo_Documento').text

                    # Valida el número de identificación vacío
                    numero_identificacion = root.find('Documento_Identidad')
                    if numero_identificacion.text == None:
                        numero_identificacion.text = '0'

                    clave += numero_identificacion.text
                    # Elimina carateres de control
                    primer_nombre = root.find('Primer_Nombre')
                    segundo_nombre = root.find('Segundo_Nombre')
                    primer_apellido = root.find('Primer_Apellido')
                    segundo_apellido = root.find('Segundo_Apellido')
                    grupo_etnico = root.find('GrupoEtnico')

                    if primer_nombre.text != None:
                        root.find('Primer_Nombre').text = primer_nombre.text.replace(
                            '\t', ' ')
                        clave += primer_nombre.text
                    if segundo_nombre.text != None:
                        root.find('Segundo_Nombre').text = segundo_nombre.text.replace(
                            '\t', ' ')
                        clave += segundo_nombre.text
                    if primer_apellido.text != None:
                        root.find('Primer_Apellido').text = primer_apellido.text.replace(
                            '\t', ' ')
                        clave += primer_apellido.text
                    if segundo_apellido.text != None:
                        root.find('Segundo_Apellido').text = segundo_apellido.text.replace(
                            '\t', ' ')
                        clave += segundo_apellido.text

                    # Eliminación de la tilde
                    if grupo_etnico.text != None and grupo_etnico.text == 'Indígena':
                        grupo_etnico.text = 'Indigena'

                    if root.find('Tipo').text == 'Persona_Natural':

                        # Relaciona la clave intermedia SOLO PARA PERSONAS NATURALES
                        if clave not in interesados_clave:
                            interesados_clave[clave] = root.find('id').text
                            interesados_normal[root.find(
                                'id').text] = root.find('id').text
                        else:
                            interesados_normal[root.find(
                                'id').text] = interesados_clave[clave]
                            continue
                    else:
                        interesados_normal[root.find(
                            'id').text] = root.find('id').text
                    interesados_procesados.append(root.find('id').text)

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('id'))
                    root.remove(root.find('predio_id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_AgrupacionInteresados' in root_tag[0]:
                    predio = root.find('predio_id').text
                    if predio in grupo_interesados_queue:
                        grupo_interesados_queue[predio].append(
                            root.find('persona_id').text)
                        continue
                    else:
                        grupo_interesados_queue[predio] = [
                            root.find('persona_id').text]

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('id'))
                    root.remove(root.find('predio_id'))
                    root.remove(root.find('persona_id'))

                if 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_DatosPHCondominio' in root_tag[0]:
                    util.agregar_tag_ric_predio_datos_ph(
                        root, root.find('predio_id').text, predios_uuid)

                    # Elimina atributos que no son parte del reporte
                    root.remove(root.find('predio_id'))

                if ('Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_FuenteAdministrativa' not in root_tag[0]
                        and 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_TramiteCatastral' not in root_tag[0]
                        and 'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_DatosPHCondominio' not in root_tag[0]):
                    util.agregar_tag_vida_util_version(root)

                output.write(ET.tostring(
                    root, encoding='unicode', method='xml') + '\n')

            fin = time.time()
            b.logging.info(f'Tiempo de escritura XTF: {str(fin - inicio)} {SEGUNDOS} para id municipio -> '
                           f'{municipio_codigo}')

        if not municipio_omitido:
            # Escribe las CaracteristicasUnidadConstruccion pendientes
            b.logging.info(
                'Escribiendo la relación RIC_CaracteristicasUnidadConstruccion...')
            util.agregar_tag_caracteristicasunidadconstruccion(
                caracteristicas_cons_queue, unidades_omitidas, output)

            # Agrega la relacion ueBaunit entre los registros de predio, terreno, construccion y unidadconstruccion
            b.logging.info('Escribiendo la relación col_ueBaunit...')
            util.agregar_tag_ue_baunit(ue_baunit_queue, predios_uuid, output)

            # Agrega la relacion col_rrrFuente entre los registros de derecho y fuente_administrativa
            b.logging.info('Escribiendo la relación col_rrrFuente...')
            util.agregar_tag_col_rrr_fuente(
                col_rrr_fuente_queue, fuentes_uuid, output)

            # Agrega la relacion predio_tramitecatastral entre los registros de tramitecatastral y predio
            b.logging.info(
                'Escribiendo la relación ric_predio_tramitecatastral...')
            util.agregar_tag_ric_predio_tramitecatastral(
                predio_tramitecatastral_queue, predios_uuid, output)

            # Agrega la relacion ric_predio_copropiedad entre los predios matriz y sus unidades prediales
            b.logging.info('Escribiendo la relación ric_predio_copropiedad...')
            errores_matriz = util.agregar_tag_predio_copropiedad(
                predio_copropiedad_queue, predios_uuid, matrices_uuid, output)
            errores += errores_matriz

            # Agrega la relacion col_miembros entre el interesado y la agrupación de derecho a la que pertenece
            b.logging.info('Escribiendo la relación col_miembros...')
            util.agregar_tag_col_miembros(
                grupo_interesados_queue, interesados_uuid, interesados_normal, agrup_inter_uuid, output)

            # Agrega la relacion col_unidadfuente entre la FuenteAdministrativa y el Predio
            b.logging.info('Escribiendo la relación col_unidadfuente...')
            util.agregar_tag_col_unidadfuente_queue(
                col_unidadfuente_queue, predios_uuid, output)

            # Agrega la relacion ric_predio_informalidad
            b.logging.info(
                'Escribiendo la relación ric_predio_informalidad...')
            util.agregar_tag_ric_predio_informalidad(
                predio_informalidad, predios_uuid_num_predial, output)

            # Cierre del fichero
            util.set_cierre_cabecera(output)
            output.close()
            
    except Exception as error:
        b.logging.error(error)

    finally:
        # database.close_conn()
        total_end = time.time()
        b.logging.info(
            f'Tiempo total de ejecución: {str(total_end - total_inicio)} {SEGUNDOS} para id municipio -> '
            f'{municipio_codigo}')

    return errores
