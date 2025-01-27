from sqlalchemy import Column, Integer, Date, Boolean, ForeignKey, Sequence, String

from database import Base


class Ejecuciones(Base):
    __tablename__: str = 'EJECUCIONES'
    __table_args__: dict = {'schema': 'SNC_GENERALES'}

    id = Column(Integer, Sequence("EJECUCIONES_SEQ", schema="SNC_GENERALES"), primary_key=True)
    id_disparador = Column(Integer, ForeignKey('SNC_GENERALES.DISPARADORES.id'))
    id_estado = Column(Integer)
    fecha_ejecucion = Column(Date)
    fecha_finalizacion = Column(Date)
    exitoso = Column(Boolean)
    registros_con_errores = Column(Integer)
    ruta_logs_de_errores = Column(String)

