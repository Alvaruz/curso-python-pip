from config.database import Base
from sqlalchemy import Column, String, Integer, Float

class DatosPostgres(Base):
    __tablename__ = ""

    id = Column(Integer, primary_key=True)
    title = Column(String)
    overview = Column(String)
    year = Column(Integer)
    rating = Column(Float)
    category = Column(String)