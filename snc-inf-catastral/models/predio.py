from sqlalchemy import Column, Integer

from database import Base


class Predio(Base):
    __tablename__: str = 'PREDIO'
    __table_args__: dict = {'schema': 'SNC_CONSERVACION'}

    id = Column(Integer, primary_key=True)
    municipio_codigo = Column(Integer)
