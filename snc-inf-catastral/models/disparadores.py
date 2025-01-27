from sqlalchemy import Column, Integer, String, Date, Boolean

from database import Base


class Disparadores(Base):
    __tablename__: str = 'DISPARADORES'
    __table_args__: dict = {'schema': 'SNC_GENERALES'}

    id = Column(Integer, primary_key=True)
    id_municipio = Column(String)
    activo = Column(Boolean)
    fecha_ejecucion = Column(Boolean)
    bimestre = Column(Integer)
