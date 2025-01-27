from sqlalchemy import Column, Integer

from database import Base


class ErroresConsecutivos(Base):
    __tablename__: str = 'ERRORES_CONSECUTIVOS'
    __table_args__: dict = {'schema': 'SNC_GENERALES'}

    id = Column(Integer, primary_key=True)
    id_predio = Column(Integer)
    errores = Column(Integer)
