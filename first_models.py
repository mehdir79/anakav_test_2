

from typing import Optional , Dict 
from sqlalchemy import String , ForeignKey , event  , select , func , create_engine , UniqueConstraint ,JSON,null
from sqlalchemy.orm import DeclarativeBase ,Mapped , mapped_column  , sessionmaker ,Session



class Base(DeclarativeBase):
    pass

engine = create_engine("sqlite:///database.db" ,echo=True)
Session1 = sessionmaker(bind=engine)
session = Session1()



class cities(Base):
    __tablename__ = "cities"

    city_id : Mapped[int] = mapped_column( autoincrement=True , primary_key= True , unique= True)
    name : Mapped[str] = mapped_column(String(50) , unique=True )
    code_omor : Mapped[int] = mapped_column( unique=True )


    def __init__(self , name , code_omor):
        self.name = name
        self.code_omor = code_omor

class test_ref(Base):
    __tablename__ = "tests_refs"

    test_id : Mapped[int] = mapped_column( autoincrement=True , primary_key= True , unique= True)
    test_name : Mapped[str] = mapped_column(String(50) , unique=True )
    test_num : Mapped[int] = mapped_column(unique=True)
    majmo_name : Mapped[str] = mapped_column()
    additional_attrs: Mapped[Optional[Dict[str, str]]] = mapped_column(JSON,default=None)
    


    def __init__(self , name , test_num ,majmo_name, additional):
        self.test_name = name
        self.test_num = test_num
        self.majmo_name = majmo_name
        self.additional_attrs = additional



def get_test_by_num(test_num: int, db: Session):
    stmt = select(test_ref).where(test_ref.test_num == test_num)
    result = db.execute(stmt).scalars().first()
    return result

class tests(Base):
    __tablename__ = "tests"
    test_id :Mapped[int]= mapped_column(autoincrement=True ,unique=True , primary_key=True)
    city_name : Mapped[int] = mapped_column(ForeignKey("cities.name",onupdate="CASCADE", ondelete="CASCADE"))
    test_num : Mapped[int] = mapped_column()
    year : Mapped[int] = mapped_column()
    month : Mapped[int] = mapped_column()
    dardast_ejra : Mapped[int] = mapped_column()
    tahie_soorat_vaziat : Mapped[int] = mapped_column()
    soorat_vaziat_setad : Mapped[int] = mapped_column()
    soorat_vaziat_mali : Mapped[int] = mapped_column()
    additional_attrs : Mapped[Optional[Dict[str,int]]] = mapped_column(JSON ,nullable=True)
    __table_args__ = (
        UniqueConstraint("city_name", "year", "month" , "test_num", name="uq_city_year_month_test"),
    )

    
    def __init__(self , city_name ,test_num, year , month , dardast_ejra,tahie_soorat_vaziat,soorat_vaziat_setad ,soorat_vaziat_mali , additional):
        adds = get_test_by_num(test_num, session)
        if adds:
            attrs = adds.additional_attrs
            if attrs is not None:
                if attrs is not null:
                    if additional is not None:
                        
                        if all(k in additional for k in attrs.keys()):
                            self.additional_attrs = {k: additional[k] for k in attrs.keys()}
                        else:
                            print(attrs)
                            raise ValueError("Expected 'additional' to provide required keys.")
                    else:
                        raise ValueError("Expected 'additional' dict.")
                elif additional == None:
                    self.additional_attrs = None
                    
        else:
            raise ValueError("Test reference not found for test_num.")

        
        self.city_name = city_name
        self.test_num = test_num
        self.year = year
        self.month = month
        self.dardast_ejra = dardast_ejra
        self.tahie_soorat_vaziat = tahie_soorat_vaziat
        self.soorat_vaziat_setad = soorat_vaziat_setad
        self.soorat_vaziat_mali = soorat_vaziat_mali
        


Base.metadata.create_all(bind=engine)

'''        if adds:
            attrs = adds.additional_attrs
            if attrs != {}:
                if additional is not None:
                    sum_add = 0
                    for key in attrs.keys():
                        sum_add += additional[key]
                        self.additional_attrs[key] = additional[key]
                    self.majmo_test = sum_add + dardast_ejra + tahie_soorat_vaziat + soorat_vaziat_setad + soorat_vaziat_mali

            else:
                self.majmo_test = dardast_ejra + tahie_soorat_vaziat + soorat_vaziat_setad + soorat_vaziat_mali
        
'''
