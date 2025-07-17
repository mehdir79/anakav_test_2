from fastapi import APIRouter , Query
import pandas as pd
from typing import Dict , List , Optional
from sqlalchemy import create_engine ,and_
from sqlalchemy.orm import sessionmaker 
from first_models import cities , test1 ,test2 , test3 , test4 , test5 , test6 , test7 , majmo
import traceback

router = APIRouter()
# engine = create_engine("mysql+pymysql://root:1234@localhost/test")
engine = create_engine("sqlite:///database.db")
Session1 = sessionmaker(autoflush= False ,  autocommit = False,bind=engine)


def read():
    with Session1.begin() as con:
        queries : Dict[str , list] = {}
        for model in [cities , test1 , test2 , test3 , test4 , test5 , test6 , test7 , majmo]:
            queries[model.__name__] = con.query(model).all()
        
        columns : Dict[str , list] = {}
        for model in [cities , test1 , test2 , test3 , test4 , test5 , test6 , test7 , majmo]:
            columns[model.__tablename__] = [col for col in model.__table__.columns]
        
        dataframes : Dict[str , pd.DataFrame] = {}
        for model in [cities , test1 , test2 , test3 , test4 , test5 , test6 , test7 , majmo]:
            dataframes[model.__tablename__] = pd.DataFrame([
            {col.name: getattr(row, col.key) for col in columns[model.__tablename__]}
            for row in queries[model.__name__]
        ])
        
        multi_cols:Dict[str,list] = {}
        
        for test in [test1,test2,test3 , test4 , test5 , test6 , test7 , majmo]:
            multi_cols[test.__tablename__] = []
            for col in test.__table__.columns:
                multi_cols[test.__tablename__].append(tuple([test.__tablename__ , col.name]))
        for test in [test1,test2,test3 , test4 , test5 , test6 , test7 , majmo]:
        
            dataframes[test.__tablename__].drop("آیدی" , axis = 1 , inplace = True)
            dataframes[test.__tablename__].set_index(["نام شهر", "سال","ماه"] , inplace=True)
            dataframes[test.__tablename__].columns = pd.MultiIndex.from_tuples(multi_cols[test.__tablename__][4:])
        
        df = dataframes[test1.__tablename__]
        for test in [test2,test3 , test4 , test5 , test6 , test7 , majmo]:
            df = df.join(dataframes[test.__tablename__] ,how="outer")
        return df

def get_cities_df():
    with Session1.begin() as con:
        cities_query = con.query(cities).all()
        df = pd.DataFrame([
            {col.name: getattr(city, col.key) for col in cities.__table__.columns}
            for city in cities_query
        ])
        df.drop(cities.__table__.columns[0].name , axis= 1 , inplace= True)
        return df


@router.get("/cities/get_cities")
def get_cities():
    all_city = get_cities_df()
    return all_city.to_dict(orient="records")

@router.post("/cities/add_city")
def add_city(city_name : str , code_omor : int):
    new_city = cities(name = city_name , code_omor = code_omor)
    with Session1.begin() as con:
        try:
            con.add(new_city)
            con.commit()
            return {"massage" : "city added"}
        except Exception as e:
            print("خطا:")
            print(e)
            traceback.print_exc()
            return {"message": f"something went wrong: {str(e)}"}
        
@router.delete("/cities/delete_city")
def delete_city(city_name: str, code_omor: int):
    with Session1.begin() as con:
        # حذف رکوردهای تست‌ها
        for model in [test1, test2, test3, test4, test5, test6, test7]:
            records = con.query(model).filter(model.city_name == city_name).all()
            if len(records) != 0: 
                for rec in records:
                    con.delete(rec)
        mj = con.query(majmo).filter(majmo.city_name == city_name).all()
        if len(mj) != 0:
            for i in mj:
                con.delete(i)


    with Session1.begin() as con:
        # حذف رکورد شهر
        city = con.query(cities).filter(
            cities.name == city_name,
            cities.code_omor == code_omor
        ).first()
        
        if city is not None:
            con.delete(city)
            return {"message": "city Deleted"}
        else:
            return {"message": "city not found"}


@router.put("/cities/edit_city")
def edit_city(city_name : str , code_omor : int , new_name : str ,  new_code_omor :int):
    with Session1.begin() as con:
        city = con.query(cities).where(cities.name == city_name and cities.code_omor == code_omor).first()
        if city is not None:
            city.name = new_name
            city.code_omor = new_code_omor
            con.commit()
            return{"massage":"city Edited"}
        else:
            return{"massage":"city not found"}
    

@router.get("/tests/getall")
def get_all():
    data = read()
    cols = []
    for column in data.columns:
        if column[1] == "majmo_test_7":
            cols.append(column[0][-5:] + " " + 'مجموع دستورکارهای باز توسعه و احداث و اصلاح و روشنایی معابر و برگشتی و طرح جامع بم')
        else:
            cols.append(column[0][-5:] + " " + column[1])
        
    data.columns = cols
    data.reset_index(inplace=True)
    data = data.to_dict(orient="dict")
    return data


@router.get("/tests/specific_test_records")
def get_specific_test(testnames : List[str] = Query(default = [test1.__tablename__ , test2.__tablename__ , test3.__tablename__ , test4.__tablename__ , test5.__tablename__ , test6.__tablename__ ,test7.__tablename__ , majmo.__tablename__  ]),
                      city_name:Optional[str] = Query(default=None)) -> dict:
    data : pd.DataFrame = read()
    if len(testnames) == 1:
        df = data[testnames[0]].reset_index()
        if city_name is not None:
            return df[:][df["نام شهر"] == city_name].to_dict()
        else:
            return df.to_dict()
    
    elif len(testnames) > 1:
        s_data = data[testnames]
        cols = []
        for column in s_data.columns:
            if column[1] == "majmo_test_7":
                cols.append(column[0][-5:] + " " + 'مجموع دستورکارهای باز توسعه و احداث و اصلاح و روشنایی معابر و برگشتی و طرح جامع بم')
            else:
                cols.append(column[0][-5:] + " " + column[1])    
        s_data.columns = cols
        s_data.reset_index(inplace=True)
        if city_name is not None:
            return s_data[:][s_data["نام شهر"] == city_name].to_dict(orient="dict")
        else:
            return s_data.to_dict(orient="dict")
    else:
        return {"massage" : "something went wrong"}



@router.post("/tests/add_test_record")
def add_test(city_name : str = Query(),
            year : int = Query(),
            month: int = Query(),
            dardast_ejra1: int = Query(description="دردست اجرا تست 1:"),
            tahie_soorat_vaziat1 : int = Query(description="تهیه صورت وضعیت تست 1"),
            soorat_vaziat_setad1 : int = Query(description="صورت وضعیت نزد ستاد تست 1"),
            soorat_vaziat_mali1:int = Query(description="صورت وضعیت نزد مالی تست 1"),
            dardast_ejra2: int = Query(description="دردست اجرا تست 2:"),
            tahie_soorat_vaziat2 : int = Query(description="تهیه صورت وضعیت تست 2"),
            soorat_vaziat_setad2 : int = Query(description="صورت وضعیت نزد ستاد تست 2"),
            soorat_vaziat_mali2 : int = Query(description="صورت وضعیت نزد مالی تست 2"),
            dardast_ejra3 : int = Query(description="دردست اجرا تست 3:"),
            tahie_soorat_vaziat3 : int = Query(description="تهیه صورت وضعیت تست 3"),
            soorat_vaziat_setad3 : int = Query(description="صورت وضعیت نزد ستاد تست 3"),
            soorat_vaziat_mali3:int = Query(description="صورت وضعیت نزد مالی تست 3"),
            dardast_ejra4: int = Query(description="دردست اجرا تست 4:"),
            tahie_soorat_vaziat4 : int = Query(description="تهیه صورت وضعیت تست 4"),
            soorat_vaziat_moshaver4 : int = Query(description="صورت وضعیت نزد مشاور تست 4):"),
            soorat_vaziat_setad4 : int = Query(description="صورت وضعیت نزد ستاد تست 4"),
            soorat_vaziat_mali4:int = Query(description="صورت وضعیت نزد مالی تست 4"),
            dardast_ejra5: int = Query(description="دردست اجرا تست 5:"),
            tahie_soorat_vaziat5 : int = Query(description="تهیه صورت وضعیت تست 5"),
            soorat_vaziat_moshaver5 : int = Query(description="صورت وضعیت نزد مشاور تست 5):"),
            soorat_vaziat_setad5 : int = Query(description="صورت وضعیت نزد ستاد تست 5"),
            soorat_vaziat_mali5:int = Query(description="صورت وضعیت نزد مالی تست 5"),
            dardast_ejra6: int = Query(description="دردست اجرا تست 6:"),
            tahie_soorat_vaziat6 : int = Query(description="تهیه صورت وضعیت تست 6"),
            soorat_vaziat_moshaver6 : int = Query(description="صورت وضعیت نزد مشاور تست 6):"),
            soorat_vaziat_setad6 : int = Query(description="صورت وضعیت نزد ستاد تست 6"),
            soorat_vaziat_mali6:int = Query(description="صورت وضعیت نزد مالی تست 6"),
            dardast_ejra7: int = Query(description="دردست اجرا تست 7:"),
            tahie_soorat_vaziat7 : int = Query(description="تهیه صورت وضعیت تست 7"),
            soorat_vaziat_moshaver7 : int = Query(description="صورت وضعیت نزد مشاور تست 7):"),
            soorat_vaziat_setad7 : int = Query(description="صورت وضعیت نزد ستاد تست 7"),
            soorat_vaziat_mali7:int = Query(description="صورت وضعیت نزد مالی تست 7"),
            ):
    with Session1.begin() as con:
        try:
            new_t1 = test1(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra1 , tahie_soorat_vaziat=tahie_soorat_vaziat1 , soorat_vaziat_setad=soorat_vaziat_setad1 , soorat_vaziat_mali=soorat_vaziat_mali1)
            new_t2 = test2(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra2 , tahie_soorat_vaziat=tahie_soorat_vaziat2 , soorat_vaziat_setad=soorat_vaziat_setad2 , soorat_vaziat_mali=soorat_vaziat_mali2)
            new_t3 = test3(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra3 , tahie_soorat_vaziat=tahie_soorat_vaziat3 , soorat_vaziat_setad=soorat_vaziat_setad3 , soorat_vaziat_mali=soorat_vaziat_mali3)
            new_t4 = test4(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra4 , tahie_soorat_vaziat=tahie_soorat_vaziat4 ,soorat_vaziat_moshaver=soorat_vaziat_moshaver4, soorat_vaziat_setad=soorat_vaziat_setad4 , soorat_vaziat_mali=soorat_vaziat_mali4)
            new_t5 = test5(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra5 , tahie_soorat_vaziat=tahie_soorat_vaziat5 ,soorat_vaziat_moshaver=soorat_vaziat_moshaver5, soorat_vaziat_setad=soorat_vaziat_setad5 , soorat_vaziat_mali=soorat_vaziat_mali5)
            new_t6 = test6(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra6 , tahie_soorat_vaziat=tahie_soorat_vaziat6 ,soorat_vaziat_moshaver=soorat_vaziat_moshaver6, soorat_vaziat_setad=soorat_vaziat_setad6 , soorat_vaziat_mali=soorat_vaziat_mali6)
            new_t7 = test7(city_name=city_name,year=year , month=month ,dardast_ejra=dardast_ejra7 , tahie_soorat_vaziat=tahie_soorat_vaziat7 ,soorat_vaziat_moshaver=soorat_vaziat_moshaver7, soorat_vaziat_setad=soorat_vaziat_setad7 , soorat_vaziat_mali=soorat_vaziat_mali7)
            con.add_all([new_t1,new_t2,new_t3,new_t4,new_t5,new_t6,new_t7])
            con.commit()
            return{"massage":"test record added"}
        except Exception as e:
            print("خطا هنگام افزودن رکورد:")
            print(e)
            traceback.print_exc()
            return {"message": f"something went wrong: {str(e)}"}
        
@router.post("/tests/add_one_test_record_NOT_RECOMMENDED")
def add_test_NR(desc : None = Query(description="از این روت استفاده نکنید چون فقط یک رکورد برای یک تست اضافه میکند و باعث میشود که وقتی از روت tests/get_all میخواهید استفاده کنید با خطا روبرو شوید زیرا بقیه تست ها برای این شهر و ماه و سال خاص هیچ رکوردی ندارند بنابر این مقدار نال میشود و خطا میدهد"),
    testname : str = Query(description=f"جدول را انتخاب کنید:{test1.__tablename__} \n {test2.__tablename__} \n {test3.__tablename__} \n {test4.__tablename__} \n {test5.__tablename__} \n {test6.__tablename__}\n{test7.__tablename__}") ,
            city_name : str = Query(),
            year : int = Query(),
            month: int = Query(),
            new_dardast_ejra: int = Query(description="دردست اجرا:"),
            new_tahie_soorat_vaziat : int = Query(description="تهیه صورت وضعیت"),
            new_soorat_vaziat_moshaver : Optional[int] = Query(default=None , description="صورت وضعیت نزد مشاور(فقط برا تست هل 4و5و6و7):"),
            new_soorat_vaziat_setad : int = Query(description="صورت وضعیت نزد ستاد"),
            new_soorat_vaziat_mali:int = Query(description="صورت وضعیت نزد مالی")
            ):
    with Session1.begin() as con:
        w = 0
        for test in [test1 , test2 , test3 , test4 , test5 , test6 , test7 ]:
            if test.__tablename__ == testname:
                w +=1
                try:
                    if hasattr(test , "soorat_vaziat_moshaver"):
                        new_test_rec = test(city_name = city_name,year = year , month =month ,dardast_ejra=new_dardast_ejra , tahie_soorat_vaziat=new_tahie_soorat_vaziat,soorat_vaziat_moshaver=new_soorat_vaziat_moshaver , soorat_vaziat_setad=new_soorat_vaziat_setad,soorat_vaziat_mali=new_soorat_vaziat_mali )
                        con.add(new_test_rec)
                        con.commit()
                    else:
                        new_test_rec = test(city_name = city_name,year = year , month =month ,dardast_ejra=new_dardast_ejra , tahie_soorat_vaziat=new_tahie_soorat_vaziat , soorat_vaziat_setad=new_soorat_vaziat_setad,soorat_vaziat_mali=new_soorat_vaziat_mali )
                        con.add(new_test_rec)
                        con.commit()
                    return {"message": "record added succeccfully"}
                except Exception as e:
                    print("خطا هنگام ویرایش رکورد:")
                    print(e)
                    traceback.print_exc()
                    return {"message": f"something went wrong: {str(e)}"}
        if w == 0:
            return {"massage": "test Not found"}


@router.put("/tests/edit_test_record")
def edit_test(testname : str = Query(description=f"جدول را انتخاب کنید:{test1.__tablename__} \n {test2.__tablename__} \n {test3.__tablename__} \n {test4.__tablename__} \n {test5.__tablename__} \n {test6.__tablename__}\n{test7.__tablename__}") ,
            city_name : str = Query(),
            year : int = Query(),
            month: int = Query(),
            new_dardast_ejra: int = Query(description="دردست اجرا:"),
            new_tahie_soorat_vaziat : int = Query(description="تهیه صورت وضعیت"),
            new_soorat_vaziat_moshaver : Optional[int] = Query(default=None , description="صورت وضعیت نزد مشاور(فقط برا تست هل 4و5و6و7):"),
            new_soorat_vaziat_setad : int = Query(description="صورت وضعیت نزد ستاد"),
            new_soorat_vaziat_mali:int = Query(description="صورت وضعیت نزد مالی")
            ):
    with Session1.begin() as con:
        w = 0
        for test in [test1 , test2 , test3 , test4 , test5 , test6 , test7 ]:
            if test.__tablename__ == testname:
                w +=1
                try:
                    rec = con.query(test).where(and_(test.city_name == city_name , test.year == year , test.month == month)).first()
                    if rec:
                        rec.dardast_ejra = new_dardast_ejra
                        rec.tahie_soorat_vaziat = new_tahie_soorat_vaziat
                        rec.soorat_vaziat_setad = new_soorat_vaziat_setad
                        rec.soorat_vaziat_mali = new_soorat_vaziat_mali
                        if hasattr(rec, "soorat_vaziat_moshaver"):
                            rec.soorat_vaziat_moshaver = new_soorat_vaziat_moshaver
                        
                        con.commit()
                        return{"massage":"successfully edited record"}
                    else:
                        return{"massage" : "record not found"}
                except Exception as e:
                    print("خطا هنگام ویرایش رکورد:")
                    print(e)
                    traceback.print_exc()
                    return {"message": f"something went wrong: {str(e)}"}
        if w == 0:
            return {"massage": "test Not found"}

@router.delete("/tests/delete_one_test_record_NOT_RECOMMENDED")
def delete_record_NR(testname : str = Query(description=f"جدول را انتخاب کنید:\n {majmo.__tablename__} \n {test1.__tablename__} \n {test2.__tablename__} \n {test3.__tablename__} \n {test4.__tablename__} \n {test5.__tablename__} \n {test6.__tablename__}\n{test7.__tablename__}") ,
            city_name : str = Query(),
            year : int = Query(),
            month: int = Query()):
    w = 0
    with Session1.begin()as con:
        for test in [test1 , test2 , test3 , test4 , test5 , test6 , test7 ,majmo]:
            if test.__tablename__ == testname:
                w +=1
                try:
                    rec = con.query(test).where(and_(test.city_name == city_name , test.year == year , test.month == month)).first()
                    if rec:
                        con.delete(rec)
                        con.commit()
                        return{"massage" : "successfully deleted"}
                    else:
                        return{"massage" : "record not found"}
                except Exception as e:
                    print("خطا هنگام ویرایش رکورد:")
                    print(e)
                    traceback.print_exc()
                    return {"message": f"something went wrong: {str(e)}"}
        if w == 0:
            return{"massage" : "table not found"}
        

@router.delete("/tests/delete_test_record")
def delete_record(
            city_name : str = Query(),
            year : int = Query(),
            month: int = Query()):
    try:
        with Session1.begin()as con:
            for model in [test1, test2, test3, test4, test5, test6, test7]:
                records = con.query(model).where(and_(model.city_name == city_name , model.year == year , model.month == month)).all()
                if len(records) != 0: 
                    for rec in records:
                        con.delete(rec)
            con.commit()
        with Session1.begin() as con1:
            recmajmo = con1.query(majmo).where(and_(majmo.city_name == city_name , majmo.year == year , majmo.month == month)).all()
            if len(recmajmo) != 0:
                for recm in recmajmo:
                    con1.delete(recm)
            con1.commit()
            return{"massage" : "successfully deleted"}
    except Exception as e:
        print("خطا هنگام حذف رکورد:")
        print(e)
        traceback.print_exc()
        return {"message": f"something went wrong: {str(e)}"}  
          