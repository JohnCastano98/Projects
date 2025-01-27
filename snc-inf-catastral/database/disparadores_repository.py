from datetime import datetime

from sqlalchemy import and_

import database
from models.disparadores import Disparadores


def find_by_date_to_execute(fecha_a_buscar: datetime) -> list:
    """
    Funcion para buscar los disparadores en la base de datos

    :param datetime fecha_a_buscar:
        fecha en la cual se buscaran los disparadores en la base de datos

    :return:
        lista de registros encontrados en la base de datos
    """
    query = database.session.query(Disparadores).filter(and_(Disparadores.activo == True,
                                                             Disparadores.fecha_ejecucion == fecha_a_buscar)).order_by(Disparadores.id)
    return query.all()
