import database
from models.reglas_validacion import ReglasValidacion


def find_active_rules():
    """
    Funcion para consultar la reglas de validacion activas en la base de datos

    :return: (list)
        Lista de objetos correspondientes a los registros encontrados
    """
    query = database.session.query(ReglasValidacion).filter(ReglasValidacion.activa == True)
    return query.all()
