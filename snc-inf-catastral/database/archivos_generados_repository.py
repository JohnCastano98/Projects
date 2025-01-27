from sqlalchemy.orm import Session

from database import engine
from models.archivos_generados import ArchivosGenerados


def insert(archivo: ArchivosGenerados):
    with Session(engine) as session:
        session.add(archivo)
        session.commit()
