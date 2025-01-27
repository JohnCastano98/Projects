from sqlalchemy import Column, Integer, ForeignKey, String, Boolean, Sequence

from database import Base


class ArchivosGenerados(Base):
    __tablename__: str = 'ARCHIVOS_GENERADOS'
    __table_args__: dict = {'schema': 'SNC_GENERALES'}

    id = Column(Integer, Sequence("ARCHIVOS_SEQ", schema="SNC_GENERALES"), primary_key=True)
    id_ejecucion = Column(Integer, ForeignKey('SNC_GENERALES.EJECUCIONES.id'))
    ruta = Column(String)
    eliminado = Column(Boolean)
