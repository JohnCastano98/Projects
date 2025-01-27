from datetime import datetime

from sqlalchemy.orm import Session

from database import engine
from models.ejecuciones import Ejecuciones


class EjecucionesRepository:

    SESSION = Session(engine)
    ejecucion: Ejecuciones

    def construir_diccionario_ejecuciones(self) -> dict:
        """
            Funcion que construye el diccionario equivalente al objeto de ejecuciones

        :return: (dict)
            Diccionario correspondiente a los datos del objeto ejecuciones
        """
        return {
            'id': self.ejecucion.id,
            'id_disparador': self.ejecucion.id_disparador,
            'id_estado': self.ejecucion.id_estado,
            'fecha_ejecucion': self.ejecucion.fecha_ejecucion,
            'fecha_finalizacion': self.ejecucion.fecha_finalizacion,
            'exitoso': self.ejecucion.exitoso
        }

    def insert(self, ejecucion: Ejecuciones) -> dict:
        """
        Funcion para insertar un registro en la entidad EJECUCIONES

        :param Ejecuciones ejecucion:
            Objeto del registro a insertar en la BD

        :return: (dict)
            Diccionario del objeto almacenado
        """
        self.SESSION.add(ejecucion)
        self.SESSION.commit()
        self.ejecucion = ejecucion
        return self.construir_diccionario_ejecuciones()

    def update_success(self, ruta_log_de_errores: str, cantidad_errores: int) -> dict:
        """
        Funcion para realizar la actualizacion del registro en la base de datos una vez finalizada la ejecucion del
        reporte

        :param (str) ruta_log_de_errores:
            Ruta del servidor SFTP donde se encuentran los logs de errores

        :param (int) cantidad_errores:
            Cantidad de registros que presentan errores

        :return: (dict)
            Diccionario con la informacion del registro actualizado
        """
        self.ejecucion.id_estado = 2
        self.ejecucion.fecha_finalizacion = datetime.now()
        self.ejecucion.exitoso = True
        self.ejecucion.ruta_logs_de_errores = ruta_log_de_errores
        self.ejecucion.registros_con_errores = cantidad_errores

        self.SESSION.flush()
        self.SESSION.commit()
        return self.construir_diccionario_ejecuciones()
