import pygeoj


def cargar_geojson(ruta: str):
    """
    Incializa el GeoJSON en una variable de tipo pygeoj

    Args:
        ruta (str): Ruta del fichero .geojson
    Returns
        pygeoj: Objeto que contiene la información del GeoJSON
    """
    return pygeoj.load(filepath=ruta)
