import math
import xml.etree.ElementTree as ET
from datetime import datetime
import codecs
import pathlib
import os
import business as b
import pygeoj

# Variables estáticas
HEADER = '''<?xml version="1.0" encoding="UTF-8"?>
<TRANSFER xmlns="http://www.interlis.ch/INTERLIS2.3">
<HEADERSECTION SENDER="ili2pg-4.5.0-fc023c8d2d8cd44d792927e45dc80c1ad973f095" VERSION="2.3">
<MODELS>
<MODEL NAME="ISO19107_PLANAS_V3_0" VERSION="2016-03-07" URI="http://www.swisslm.ch/models">¿</MODEL>
<MODEL NAME="LADM_COL_V3_1" VERSION="V1.2.0" URI="http://www.proadmintierra.info/"></MODEL>
<MODEL NAME="Modelo_Aplicacion_LADMCOL_RIC_V0_1" VERSION="V2.2.1" URI="http://www.proadmintierra.info/"></MODEL>
</MODELS>
</HEADERSECTION>
<DATASECTION>
<Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC BID="Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC">
<Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_GestorCatastral TID="bfb4f268-e49c-4392-a6d4-b04826bf4724"><Nombre_Gestor>IGAC</Nombre_Gestor><NIT_Gestor_Catastral>899999004-9</NIT_Gestor_Catastral><Fecha_Inicio_Prestacion_Servicio>2021-12-13</Fecha_Inicio_Prestacion_Servicio></Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_GestorCatastral>
<Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_OperadorCatastral TID="a4b05dbe-ed53-48fd-9d37-221525837a6d"><Nombre_Operador>IGAC</Nombre_Operador><NIT_Operador_Catastral>899999004-9</NIT_Operador_Catastral></Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.RIC_OperadorCatastral>'''

HEADER_CLOSER = '''</Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC>
</DATASECTION>
</TRANSFER>'''

QUERIES = [
    'predio.sql',
    'terreno.sql',
    'construccion.sql',
    'caracteristica_unidad_construccion.sql',
    'unidad_construccion.sql',
    'interesado.sql',
    'datos_ph_condominio.sql',
    'fuente_administrativa.sql',
    'agrupacion_interesados.sql',
    'derecho.sql',
    'tramite_catastral.sql'
]


def set_cabecera(f):
    """
    Escribe la cabecera del XTF
    Args: 
        f(File) archivo xtf en construcción
    """

    f.write(HEADER + '\n')


def set_cierre_cabecera(f):
    """
    Escribe las etiquetas de cierre de la cabecera del XTF
    Args: 
        f(File): archivo xtf en construcción
    """
    f.write(HEADER_CLOSER)


def crear_fichero_salida(municipio_codigo, fecha, vacio):
    # Se asegura de limpiar el contenido en caso de haber algo escrito, esto puede pasar
    # si se detiene una ejecución de manera forzoza
    output = codecs.open(os.path.join(
        pathlib.Path(__name__).parent.absolute(),
        'reportes',
        '{}_{}.xtf'.format(municipio_codigo, fecha.strftime("%Y-%m-%d"))), 'w', 'utf-8'
    )
    output.write('')
    output.close()

    output = codecs.open(os.path.join(
        pathlib.Path(__name__).parent.absolute(),
        'reportes',
        '{}_{}.xtf'.format(municipio_codigo, fecha.strftime("%Y-%m-%d"))), 'a', 'utf-8'
    )
    if not vacio:
        set_cabecera(output)

    return output


def validar_distancia_coordenadas(coordenadas):
    for i in range(len(coordenadas) - 1):
        # Valida distancia de los puntos usando la fórmula general
        if math.sqrt(math.pow((coordenadas[i+1][0]-coordenadas[i][0]), 2) + math.pow((coordenadas[i+1][1]-coordenadas[i][1]), 2)) < 0.001:
            return False
    return True


def generar_multipolygon(surface, poligonos):
    # Agrega las coordenadas de cada punto que construye el polígono
    for poligono in poligonos:
        # Es un polígono corriente
        boundary = ET.SubElement(surface, 'BOUNDARY')
        polyline = ET.SubElement(boundary, 'POLYLINE')
        if not validar_distancia_coordenadas(poligono):
            return False
        for coordenada in poligono:
            coord = ET.SubElement(polyline, 'COORD')
            ET.SubElement(coord, 'C1').text = str(coordenada[0])
            ET.SubElement(coord, 'C2').text = str(coordenada[1])
            ET.SubElement(coord, 'C3').text = '0.000'

    return True


def generar_polygon(polyline, coordenada):
    if not validar_distancia_coordenadas(coordenada):
        return False
    for punto in coordenada:
        coord = ET.SubElement(polyline, 'COORD')
        ET.SubElement(coord, 'C1').text = str(punto[0])
        ET.SubElement(coord, 'C2').text = str(punto[1])
        ET.SubElement(coord, 'C3').text = '0.000'

    return True


def agregar_tag_geometria(root: ET.ElementTree, dict_indices: dict, dict_geojson: pygeoj.GeojsonFile, numero_predial: str, entidad: str):
    """
    Agrega una nueva etiqueta de geometria

    Args: 
        root (ElementTree) Arbol de elementos xml
        dict_indices (dict): Diccionario que contiene el número predial y su indice en los Features del GeoJSON
        dic_geojson (pygeoj.GeojsonFile): Diccionario que contiene el predio y sus geometrías asociadas
        numero_predial (str): Número predial con el que se extrae la geometría del diccionario
    """

    try:
        # Extrae el índice del feature
        indice_feature = dict_indices[entidad][numero_predial]
        if indice_feature is None:
            return False

        feature = dict_geojson.get_feature(indice_feature)
        if feature is None:
            return False

        properties = feature.properties
        geometry = feature.geometry
        if len(geometry.coordinates) == 0:
            b.logging(
                f'No se ha encontrado geometría para la {entidad} de {numero_predial}')
            return False

        # Genera la estructura de árbol para un geometría
        geometria = ET.SubElement(root, 'Geometria')
        surface3_d = ET.SubElement(
            geometria, 'ISO19107_PLANAS_V3_0.GM_MultiSurface3D')
        geometry_tag = ET.SubElement(surface3_d, 'geometry')
        list_value = ET.SubElement(
            geometry_tag, 'ISO19107_PLANAS_V3_0.GM_Surface3DListValue')

        if geometry.type == 'MultiPolygon':
            for cordenada in geometry.coordinates:
                # Crea el árbol de estrucutra del polígono génerico
                value = ET.SubElement(list_value, 'value')
                surface = ET.SubElement(value, 'SURFACE')
                if not generar_multipolygon(surface, cordenada):
                    return False

        elif geometry.type == 'Polygon':
            value = ET.SubElement(list_value, 'value')
            surface = ET.SubElement(value, 'SURFACE')

            for cordenada in geometry.coordinates:
                boundary = ET.SubElement(surface, 'BOUNDARY')
                polyline = ET.SubElement(boundary, 'POLYLINE')
                if not generar_polygon(polyline, cordenada):
                    return False
        else:
            return False

        return True
    except Exception:
        return False


def agregar_tag_vida_util_version(root):
    """
    Escribe la etiqueta Comienzo_Vida_Util_Version tomando como valor la fecha actual

    Args: 
        f(File): archivo xtf en construcción
    """
    ET.SubElement(
        root, 'Comienzo_Vida_Util_Version').text = str(datetime.now().isoformat())[0:23]


def agregar_tag_ue_baunit(ue_baunit_queue, predios_uuid, output):
    """
    Escribe la etiqueta col_ueBaunit que relaciona el predio y la construcción

    Args:
        ue_baunit_queue (list): Listado de objetos que contiene el TID de la construcción y su respectivo
                                id de predio relacionado
        predios_uuid (dict): Diccionario de id's de predio y su respectivo TID
        output (file): Fichero .txf en el que se escribirá la etiqueta
    """
    for ue_beaunit in ue_baunit_queue:
        ue_baunit_tag = ET.Element('LADM_COL_V3_1.LADM_Nucleo.col_ueBaunit')
        ET.SubElement(ue_baunit_tag, 'ue').set(
            'REF', ue_beaunit['ue'])
        ET.SubElement(ue_baunit_tag, 'baunit').set(
            'REF', predios_uuid[ue_beaunit['predio']])

        output.write(ET.tostring(
            ue_baunit_tag, encoding='unicode', method='xml') + '\n')


def agregar_tag_col_unidadfuente_queue(col_unidadfuente_queue, predios_uuid, output):
    for col_unidadfuente in col_unidadfuente_queue:
        ue_baunit_tag = ET.Element(
            'LADM_COL_V3_1.LADM_Nucleo.col_unidadFuente')
        ET.SubElement(ue_baunit_tag, 'fuente_administrativa').set(
            'REF', col_unidadfuente['fuente_administrativa'])
        ET.SubElement(ue_baunit_tag, 'unidad').set(
            'REF', predios_uuid[col_unidadfuente['unidad']])

        output.write(ET.tostring(
            ue_baunit_tag, encoding='unicode', method='xml') + '\n')


def agregar_relaciones_unidad_construccion(root, id, construcciones_uuid, caracteristicas_uuid):
    """
    Agrega las etiquetas ric_caracteristicasunidadconstruccion y ric_construccion dentro de
    un árbol de elementos XML

    Args:
        root (ElementTree): Árbol de elementos XML
        id (str): id de la construcción
        construcciones_uuid (dict): Diccionario de id's de construcciones y su respectivo TID
        caracteristicas_uuid (dict): Diccionario de id's de caracteristicas_unidad_construccion
                                     y su respectivo TID
    """
    try:
        ET.SubElement(root, 'ric_caracteristicasunidadconstruccion').set(
            'REF', caracteristicas_uuid[id])
    except:
        b.logging.error(
            f'No existe el id: {id} en el diccionario caracteristicas_uuid')

    try:
        ET.SubElement(root, 'ric_construccion').set(
            'REF', construcciones_uuid[id])
    except:
        b.logging.error(
            f'No existe el id: {id} en el diccionario construcciones_uuid')


def agregar_tag_col_rrr_fuente(col_rrr_fuente_queue, fuentes_uuid, output):
    """
    Escribe la etiqueta col_rrrFuente que relaciona el Derecho y la FuenteAdministrativa

    Args:
        col_rrr_fuente_queue (list): Listado de objetos que contiene el id del predio y el TID de la FuenteAdministrativa relacionada
        derecho_uuid (dict): Diccionario de id's de predios y sus respectivos TID de Derecho
        output (file): fichero .xtf
    """
    try:
        for rrr in col_rrr_fuente_queue:
            rrr_tag = ET.Element('LADM_COL_V3_1.LADM_Nucleo.col_rrrFuente')
            ET.SubElement(rrr_tag, 'fuente_administrativa').set(
                'REF', fuentes_uuid[rrr['id']])
            ET.SubElement(rrr_tag, 'rrr').set('REF', rrr['derecho'])

            output.write(ET.tostring(
                rrr_tag, encoding='unicode', method='xml') + '\n')
    except Exception:
        b.logging.error(
            f'No existe el valor: {rrr} para rrr en el diccionario fuentes_uuid')


def agregar_tag_ric_predio_tramitecatastral(predio_tramitecatastral_queue, predios_uuid, output):
    """
    Escribe la etiqueta ric_predio_tramitecatastral que relaciona el Derecho y la FuenteAdministrativa

    Args:
        col_rrr_fuente_queue (list): Listado de objetos que contiene el id del tramite y el TID del predio relacionado
        predios_uuid (dict): Diccionario de id's de predios y sus respectivos TID
        output (file): fichero .xtf
    """
    for predio_tramite in predio_tramitecatastral_queue:
        predio_tramite_tag = ET.Element(
            'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.ric_predio_tramitecatastral')
        ET.SubElement(predio_tramite_tag, 'ric_tramite_catastral').set(
            'REF', predio_tramite['tramite'])
        ET.SubElement(predio_tramite_tag, 'ric_predio').set(
            'REF', predios_uuid[predio_tramite['predio']])
        output.write(ET.tostring(
            predio_tramite_tag, encoding='unicode', method='xml') + '\n')


def agregar_tag_interesado_unidad(derecho_tag, predio_id, persona_id, predios_uuid, interesados_uuid, interesados_normal, agrup_inter_uuid, grupo_interesados_queue):
    """
    Escribe la etiqueta interesado_ric_interesado o interesado_ric_agrupacioninteresados que relaciona a uno o varios
    interesados a el derecho sobre un predio

    Args:
        derecho_tag (ElementTree): Listado de objetos que contiene el id del tramite y el TID del predio relacionado
        predio_id (str): Id del predio relacionado
        predios_uuid (dict): Diccionario de id's de RIC_Predio y sus respectivos TID
        interesados_uuid (dict): Diccionario de id's de RIC_Interesado y sus respectivos TID
        agrup_inter_uuid (dict): Diccionario de id's de RIC_Agrupacioninteresados y sus respectivos TID
        grupo_interesados_queue (list): Listado de objetos que contiene el id del predio y un listado de TIDS que refere
    """
    ET.SubElement(derecho_tag, 'unidad').set('REF', predios_uuid[predio_id])

    if predio_id in grupo_interesados_queue:
        grupo = grupo_interesados_queue[predio_id]
        ET.SubElement(derecho_tag, 'interesado').set(
            'REF', agrup_inter_uuid[predio_id])
    else:
        ET.SubElement(derecho_tag, 'interesado').set(
            'REF', interesados_uuid[interesados_normal[persona_id]])


def agregar_tag_local_id(tag, local_id):
    """
    Escribe la etiqueta Local_Id tomando como valor un uuid:

    Args:
        tag(ElementTree): Árbol de elementos XML
        local_id (str): uuid
    """
    ET.SubElement(tag, 'Local_Id').text = local_id


def agregar_tag_ric_predio(fuente_admin_tag, predio_id, predios_uuid):
    """
    Escribe la etiqueta Local_Id tomando como valor un uuid:

    Args:
        fuente_admin_tag(ElementTree): Árbol de elementos XML de RIC_FuenteAdministrativa
        predio_id (str): Id del predio
        predios_uuid (map): Diccionario de id's de predios y sus respectivos TID
    """
    ET.SubElement(fuente_admin_tag, 'unidad').text = predios_uuid[predio_id]


def agregar_tag_predio_copropiedad(predio_copropiedad_queue, predios_uuid, matrices_uuid, output):
    """
    Genera la relación entre las unidades prediales y sus respectivas matrices

    Args:
        predio_copropiedad_queue (list): Listado de objetos que contiene el id del predio matriz y el listado de TID's de sus respectivas unidades prediales
        predios_uuid (dict): Diccionario de id's de predios y sus respectivos TID
        matrices_uuid(dict): Diccionario de id's de predios matrices y sus respectivos TID
        output(File): Archivo xtf en construcción 
    """
    errores = []
    for matriz in predio_copropiedad_queue.keys():
        unidades_prediales = predio_copropiedad_queue[matriz]
        # Escritura de la matriz
        predio_co_matriz_tag = ET.Element(
            'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.ric_predio_copropiedad')
        if matriz in matrices_uuid.keys():
            ET.SubElement(predio_co_matriz_tag,
                          'ric_matriz').set('REF', matrices_uuid[matriz])
            output.write(ET.tostring(
                predio_co_matriz_tag, encoding='unicode', method='xml') + '\n')
        else:
            pass
            # Agregar error al log [numero_predial, municipio, departamente, id_error, descripción]
            errores.append(
                [matriz, '', '', '100', f'El predio matriz: {matriz} no existe'])

        # Escritura de la relación entre la matriz y sus unidades prediales
        for unidad_predial in unidades_prediales:
            predio_co_tag = ET.Element(
                'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.ric_predio_copropiedad')
            if matriz in matrices_uuid.keys():
                ET.SubElement(
                    predio_co_tag, 'ric_matriz').set('REF', matrices_uuid[matriz])
                ET.SubElement(
                    predio_co_tag, 'ric_unidad_predial').set('REF', predios_uuid[unidad_predial[0]])
                ET.SubElement(
                    predio_co_tag, 'Coeficiente').set('REF', unidad_predial[1])
                output.write(ET.tostring(
                    predio_co_tag, encoding='unicode', method='xml') + '\n')
    return errores


def agregar_tag_col_miembros(grupo_interesados_queue, interesados_uuid, interesados_normal, agrup_inter_uuid, output):
    """
    Agrega la relacion col_miembros entre el interesado y la agrupación de derecho a la que pertenece

    Args:
    grupo_interesados_queue (list):  
    interesados_uuid (dict): Diccionario de RIC_Interesados y sus respectivos TID
    agrup_inter_uuid (dict): Diccionario de RIC_Agrupacioninteresados y sus respectivos TID
    output (File): Archivo xtf en construcción
    """
    for grupo in grupo_interesados_queue.keys():
        if len(grupo_interesados_queue[grupo]) > 1:
            for interesado in grupo_interesados_queue[grupo]:
                col_miembro_tag = ET.Element(
                    'LADM_COL_V3_1.LADM_Nucleo.col_miembros')
                ET.SubElement(col_miembro_tag, 'interesado').set(
                    'REF', interesados_uuid[interesados_normal[interesado]])
                ET.SubElement(col_miembro_tag, 'agrupacion').set(
                    'REF', agrup_inter_uuid[grupo])
                output.write(ET.tostring(
                    col_miembro_tag, encoding='unicode', method='xml') + '\n')


def agregar_tag_caracteristicasunidadconstruccion(caracteristicas_cons_queue, unidades_omitidas, output):
    """
    """
    for caracteristicas in caracteristicas_cons_queue:
        if caracteristicas.find('id').text not in unidades_omitidas:
            agregar_tag_vida_util_version(caracteristicas)
            caracteristicas.remove(caracteristicas.find('id'))
            output.write(ET.tostring(
                caracteristicas, encoding='unicode', method='xml') + '\n')


def valida_anio_construccion(tag):
    """
    Valida que el Anio_Construccion se encuentre dento del dominio establcido por modelo RIC,
    en caso de no cumplir con la condición, el valor se deja en blanco

    Args:
        tag (ElementTree): Árbol de elementos XML de RIC_Construccion o RIC_CaracteristicasUnidadConstruccion
    """
    anio_cons = tag.find('Anio_Construccion').text
    if anio_cons != None and (int(anio_cons) < 1550 or int(anio_cons) > 2500):
        tag.find('Anio_Construccion').text = ''


def agregar_tag_ric_predio_datos_ph(tag_datosph, predio_id, predios_uuid):
    ET.SubElement(tag_datosph, 'ric_predio').set(
        'REF', predios_uuid[predio_id])


def agregar_tag_ric_predio_informalidad(predio_informalidad, predios_uuid, output):
    for formal in predio_informalidad:
        for informal in predio_informalidad[formal]:
            
            if formal in predios_uuid and informal in predios_uuid:

                predio_informalidad_tag = ET.Element(
                    'Modelo_Aplicacion_LADMCOL_RIC_V0_1.RIC.ric_predio_informalidad')

                ET.SubElement(predio_informalidad_tag, 'ric_predio_formal').set(
                    'REF', predios_uuid[formal])
                ET.SubElement(predio_informalidad_tag, 'ric_predio_informal').set(
                    'REF', predios_uuid[informal])

                output.write(ET.tostring(
                    predio_informalidad_tag, encoding='unicode', method='xml') + '\n')
