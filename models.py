

from typing import Optional
from sqlalchemy import String , ForeignKey , event  , select , func , create_engine , UniqueConstraint
from sqlalchemy.orm import DeclarativeBase ,Mapped , mapped_column , Session , Relationship , sessionmaker ,scoped_session
from pydantic import BaseModel



class Base(DeclarativeBase):
    pass





class cities(Base):
    __tablename__ = "cities"

    city_id : Mapped[int] = mapped_column( autoincrement=True , primary_key= True , unique= True)
    name : Mapped[str] = mapped_column(String(50) , unique=True )
    code_omor : Mapped[int] = mapped_column( unique=True )


    def __init__(self , name , code_omor):
        self.name = name
        self.code_omor = code_omor


class tests(Base):
    __tablename__ = "tests"
    test_id :Mapped[int]= mapped_column(autoincrement=True ,unique=True , primary_key=True)
    city_name : Mapped[int] = mapped_column(ForeignKey("cities.name",onupdate="CASCADE", ondelete="CASCADE"))
    test_num : Mapped[int] = mapped_column()
    year : Mapped[int] = mapped_column()
    month : Mapped[int] = mapped_column()
    dardast_ejra : Mapped[int] = mapped_column()
    tahie_soorat_vaziat : Mapped[int] = mapped_column()
    soorat_vaziat_moshaver : Mapped[int] = mapped_column( nullable= True)
    soorat_vaziat_setad : Mapped[int] = mapped_column()
    soorat_vaziat_mali : Mapped[int] = mapped_column()
    majmo_test = 0
    __table_args__ = (
        UniqueConstraint("city_name", "year", "month" , "test_num", name="uq_city_year_month_test"),
    )

    
    def __init__(self , city_name ,test_num, year , month , dardast_ejra,tahie_soorat_vaziat,soorat_vaziat_setad ,soorat_vaziat_mali , soorat_vaziat_moshaver :Optional[int]):
        self.city_name = city_name
        self.test_num = test_num
        self.year = year
        self.month = month
        self.dardast_ejra = dardast_ejra
        self.tahie_soorat_vaziat = tahie_soorat_vaziat
        self.soorat_vaziat_setad = soorat_vaziat_setad
        self.soorat_vaziat_mali = soorat_vaziat_mali
        if soorat_vaziat_moshaver is not None:
            self.soorat_vaziat_moshaver = soorat_vaziat_moshaver
            self.majmo_test = (dardast_ejra + tahie_soorat_vaziat + soorat_vaziat_setad + soorat_vaziat_mali + soorat_vaziat_moshaver)
        else:
            self.majmo_test = (dardast_ejra + tahie_soorat_vaziat + soorat_vaziat_setad + soorat_vaziat_mali)
        

engine = create_engine("sqlite:///database.db")
Base.metadata.create_all(bind=engine)