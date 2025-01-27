from sqlalchemy import Column, Integer, String, Boolean

from database import Base


class ReglasValidacion(Base):
    __tablename__: str = 'REGLAS_VALIDACION'
    __table_args__: dict = {'schema': 'SNC_GENERALES'}

    id = Column(Integer, primary_key=True)
    descripcion = Column(String)
    regla = Column(String)
    activa = Column(Boolean)