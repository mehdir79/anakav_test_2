from fastapi import APIRouter , Query
import pandas as pd
from typing import Dict , List , Optional
from sqlalchemy import create_engine ,and_ , func
from sqlalchemy.orm import sessionmaker 
from first_models import cities , tests ,test_ref
import traceback
import numpy as np
from pydantic import BaseModel

router = APIRouter()
# engine = create_engine("mysql+pymysql://root:1234@localhost/test")
engine = create_engine("sqlite:///database.db")
Session1 = sessionmaker(autoflush= False ,  autocommit = False,bind=engine)



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
        records = con.query(tests).filter(tests.city_name == city_name).all()
        if len(records) != 0: 
            for rec in records:
                con.delete(rec)
        


    with Session1.begin() as con:
        
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
        
@router.get("/tests_ref/get")
def get_tests_ref(test_num : Optional[int] = Query(None ,description="search by test_num" ) ,test_name : Optional[str] = Query(None , description="search by test_name") ):
    with Session1.begin() as con:
        if test_name is None and test_num is None:
            c_all = con.query(test_ref).all()
            cols = []
            cols.append("test_name")
            cols.append("test_num")
            cols.append("majmo_name")
            cols.append("test_id")
            cols.append("additional_attrs")
            rows_data = []
            for c in c_all:
                row_dict = {}
                for col in cols:
                    if col == "test_name":
                        row_dict["نام تست"] = c.test_name
                    elif col == "test_num":
                        row_dict["شماره تست"] = c.test_num
                    elif col == "majmo_name":
                        row_dict["نام ستون مجموع"] =c.majmo_name
                    elif col == "test_id":
                        row_dict["آیدی تست"] = c.test_id
                    else:
                        if c.additional_attrs is not None:
                            row_dict["مشخصات خاص تست"] = []
                            for key in c.additional_attrs.keys():
                                row_dict["مشخصات خاص تست"].append(c.additional_attrs[key])
                        else:
                            row_dict["مشخصات خاص تست"] = None
                rows_data.append(row_dict)
            df = pd.DataFrame(rows_data)
            return df.to_dict(orient="records")

        elif test_name:
            if test_num:
                c_all = con.query(test_ref).where(and_(test_ref.test_name == test_name , test_ref.test_num == test_num)).first()
                if c_all:  
                    cols = []
                    cols.append("test_name")
                    cols.append("test_num")
                    cols.append("majmo_name")
                    cols.append("test_id")
                    cols.append("additional_attrs")
                    rows_data = []
                    
                    row_dict = {}
                    for col in cols:
                        if col == "test_name":
                            row_dict["نام تست"] = c_all.test_name
                        elif col == "test_num":
                            row_dict["شماره تست"] = c_all.test_num
                        elif col == "majmo_name":
                            row_dict["نام ستون مجموع"] =c_all.majmo_name
                        elif col == "test_id":
                            row_dict["آیدی تست"] = c_all.test_id
                        else:
                            if c_all.additional_attrs is not None:
                                row_dict["مشخصات خاص تست"] = []
                                for key in c_all.additional_attrs.keys():
                                    row_dict["مشخصات خاص تست"].append(c_all.additional_attrs[key])
                            else:
                                row_dict["مشخصات خاص تست"] = None
                    rows_data.append(row_dict)
                    df = pd.DataFrame(rows_data)
                    return df.to_dict(orient="records")

                else:
                    return{"not found"}
            else:
                c_all = con.query(test_ref).where(test_ref.test_name == test_name).first()
                if c_all:
                    cols = []
                    cols.append("test_name")
                    cols.append("test_num")
                    cols.append("majmo_name")
                    cols.append("test_id")
                    cols.append("additional_attrs")
                    rows_data = []
                    
                    row_dict = {}
                    for col in cols:
                        if col == "test_name":
                            row_dict["نام تست"] = c_all.test_name
                        elif col == "test_num":
                            row_dict["شماره تست"] = c_all.test_num
                        elif col == "majmo_name":
                            row_dict["نام ستون مجموع"] =c_all.majmo_name
                        elif col == "test_id":
                            row_dict["آیدی تست"] = c_all.test_id
                        else:
                            if c_all.additional_attrs is not None:
                                row_dict["مشخصات خاص تست"] = []
                                for key in c_all.additional_attrs.keys():
                                    row_dict["مشخصات خاص تست"].append(c_all.additional_attrs[key])
                            else:
                                row_dict["مشخصات خاص تست"] = None
                    rows_data.append(row_dict)
                    df = pd.DataFrame(rows_data)
                    return df.to_dict(orient="records")

                return{"not found"}
        else:
            c_all = con.query(test_ref).where(test_ref.test_num == test_num).first()
            if c_all:
                cols = []
                cols.append("test_name")
                cols.append("test_num")
                cols.append("majmo_name")
                cols.append("test_id")
                cols.append("additional_attrs")
                rows_data = []
                
                row_dict = {}
                for col in cols:
                    if col == "test_name":
                        row_dict["نام تست"] = c_all.test_name
                    elif col == "test_num":
                        row_dict["شماره تست"] = c_all.test_num
                    elif col == "majmo_name":
                        row_dict["نام ستون مجموع"] =c_all.majmo_name
                    elif col == "test_id":
                        row_dict["آیدی تست"] = c_all.test_id
                    else:
                        if c_all.additional_attrs is not None:
                            row_dict["مشخصات خاص تست"] = []
                            for key in c_all.additional_attrs.keys():
                                row_dict["مشخصات خاص تست"].append(c_all.additional_attrs[key])
                        else:
                            row_dict["مشخصات خاص تست"] = None
                rows_data.append(row_dict)
                df = pd.DataFrame(rows_data)
                return df.to_dict(orient="records")

            return{"not found"}
            
@router.post("/tests_ref/add")
def add_test_ref(test_name : str , test_num : int , majmo_name : str , additional_attrs : Optional[Dict[str , str]] = None):
    try:
        new_testref = test_ref(name = test_name , test_num=test_num , majmo_name=majmo_name , additional=additional_attrs)
        with Session1.begin() as con:
            con.add(new_testref)
            con.commit()
        return{"test_ref added"}
    except:
        return{"something went wrong"}

@router.put("/tests_ref/edit")
def edit_test_ref(oldtestnum : int , test_name : str , test_num : int , majmo_name : str , additional_attrs : Optional[Dict[str , str]] = None):
    with Session1.begin() as con:
        the_test = con.query(test_ref).where(test_ref.test_num == oldtestnum).first()
        if the_test:
            try:
                the_test.test_name = test_name 
                the_test.test_num=test_num 
                the_test.majmo_name=majmo_name 
                the_test.additional_attrs=additional_attrs
                
                con.commit()
                return{"test_ref edited"}
            except:
                return{"something went wrong"}
        else:
            return{"not found"}

@router.delete("/tests_ref/edit")
def delete_test_ref(testnum : int):
    with Session1.begin() as con:
        the_test = con.query(test_ref).where(test_ref.test_num == testnum).first()
        if the_test:
            try:
                con.delete(the_test)
                con.commit()
                return{"deleted test ref"}
            except:
                return{"something went wrong"}
        else:
            return{"not found"}

@router.get("/tests/get_test")
def get_tests(test_num : Optional[int] = Query(description="search by test_num" , default=None) , city_name :Optional[str] = Query(description="search by city_name" , default=None) , year : Optional[int] = Query(default=None , description="search by year"), month:Optional[int] = Query(default=None , description="search by month")):
    with Session1.begin() as con:
        if test_num:
            if city_name:
                if year:
                    if month:
                        data =con.query(tests).where(and_(tests.city_name == city_name , tests.test_num == test_num , tests.year == year , tests.month == month)).all()
                    else:
                        data =con.query(tests).where(and_(tests.city_name == city_name , tests.test_num == test_num , tests.year == year )).all()
                elif month:
                    data =con.query(tests).where(and_(tests.city_name == city_name , tests.test_num == test_num , tests.month == month)).all()
                else:
                    data =con.query(tests).where(and_(tests.city_name == city_name , tests.test_num == test_num)).all()
            elif year:
                if month:
                    data =con.query(tests).where(and_( tests.test_num == test_num , tests.year == year , tests.month == month)).all()
                else:
                    data =con.query(tests).where(and_( tests.test_num == test_num , tests.year == year )).all()
            else:
                if month:
                    data =con.query(tests).where(and_(tests.test_num == test_num , tests.month == month)).all()
                else:
                    data =con.query(tests).where(tests.test_num == test_num).all()
        elif city_name:
            if year:
                if month:
                    data =con.query(tests).where(and_(tests.city_name == city_name , tests.year == year , tests.month == month)).all()
                else:
                    data =con.query(tests).where(and_(tests.city_name == city_name , tests.year == year)).all()
            else:
                if month:
                    data =con.query(tests).where(and_(tests.city_name == city_name , tests.month == month)).all()
                else:
                    data =con.query(tests).where(tests.city_name == city_name).all()
        elif year:
            if month:
                data =con.query(tests).where(and_(tests.year == year , tests.month == month)).all()
            else:
                data =con.query(tests).where(tests.year == year ).all()
        elif month:
            data = con.query(tests).where( tests.month == month).all()
        else:
            data = con.query(tests).where().all()
        

        collection_tests_refs = con.query(test_ref).all()
        addcols :List[str]= []
        
        
        addcols.append("city_name")
        addcols.append("test_num")
        addcols.append("year")
        addcols.append("month")
        addcols.append("dardast_ejra")
        addcols.append("tahie_soorat_vaziat")
        addcols.append("soorat_vaziat_setad")
        addcols.append("soorat_vaziat_mali")
        if data is not None:
            for test in data:
                q = test.additional_attrs
                if q is not None:
                    for x in q.keys():
                        if x not in addcols:
                            addcols.append(x)
        
        
        rows_data = []  
        
        
        for row in data:
            row_dict = {}
            for col in collection_tests_refs:
                if col.test_num == row.test_num:
                    if col.majmo_name not in addcols:
                        addcols.append(col.majmo_name)
                    for col1 in addcols:
                        sum = 0
                        if hasattr(tests, col1):
                            # چهار ستون اصلی
                            if col1 == "city_name":
                                row_dict["نام شهر"] = row.city_name
                            elif col1 == "test_num":
                                row_dict["شماره تست"] = row.test_num
                            elif col1 == "year":
                                row_dict["سال"] = row.year
                            elif col1 == "month":
                                row_dict["ماه"] = row.month
                            elif col1 == "dardast_ejra":
                                row_dict["در دست اجرا"] = row.dardast_ejra
                            elif col1 == "tahie_soorat_vaziat":
                                row_dict["تهیه صورت وضعیت"] = row.tahie_soorat_vaziat
                            elif col1 == "soorat_vaziat_setad":
                                row_dict["صورت وضعیت نزد ستاد"] = row.soorat_vaziat_setad
                            elif col1 == "soorat_vaziat_mali":
                                row_dict["صورت وضعیت نزد مالی"] = row.soorat_vaziat_mali
                            
                        elif col.additional_attrs and row.additional_attrs:
                            # ستون‌های اضافی (دینامیک)
                            for key in col.additional_attrs.keys():
                                if key == col1:
                                    row_dict[col.additional_attrs[key]] = row.additional_attrs[key]    
                                    sum += row.additional_attrs[col1]
                        sum += row.dardast_ejra + row.tahie_soorat_vaziat + row.soorat_vaziat_mali + row.soorat_vaziat_setad
                        row_dict[col.majmo_name] = sum
                        
                    # ✅ اینجا append کن، بعد از اینکه همه ویژگی‌ها اضافه شدن
                    rows_data.append(row_dict)
                    break  # فقط اولین ref که match شد کافیه
        df = pd.DataFrame(rows_data)
        df.replace([np.inf, -np.inf , np.nan], None, inplace=True)
        # df.where(pd.notnull(df), None,inplace=True) 
        return df.to_dict(orient="records")
    
@router.get("/tests/sum")
def get_sum():
    with Session1.begin() as con:
        allcols = ["city_name" , "test_num" , "year" , "month","dardast_ejra" , "tahie_soorat_vaziat" , "soorat_vaziat_setad"  , "soorat_vaziat_mali"]
        majmo_Cols = []
        testha = con.query(tests).all()
        collection_tests_refs = con.query(test_ref).all()
        for test in testha:
            if test.additional_attrs is not None:
                for key in test.additional_attrs.keys():
                    if key not in allcols:
                        allcols.append(key)
        group_cols = ["نام شهر", "شماره تست", "سال", "ماه"]
        rows_data = []
        for test in testha:
            row_dict = {}
            for col in collection_tests_refs:
                if col.test_num == test.test_num:
                    sum = 0
                    if col.majmo_name not in majmo_Cols:
                        majmo_Cols.append(col.majmo_name)
                    for col1 in allcols:
                        
                        if hasattr(tests, col1):
                            # چهار ستون اصلی
                            if col1 == "city_name":
                                row_dict["نام شهر"] = test.city_name
                            elif col1 == "test_num":
                                row_dict["شماره تست"] = test.test_num
                            elif col1 == "year":
                                row_dict["سال"] = test.year
                            elif col1 == "month":
                                row_dict["ماه"] = test.month
                            elif col1 == "dardast_ejra":
                                row_dict["در دست اجرا"] = test.dardast_ejra
                            elif col1 == "tahie_soorat_vaziat":
                                row_dict["تهیه صورت وضعیت"] = test.tahie_soorat_vaziat
                            elif col1 == "soorat_vaziat_setad":
                                row_dict["صورت وضعیت نزد ستاد"] = test.soorat_vaziat_setad
                            elif col1 == "soorat_vaziat_mali":
                                row_dict["صورت وضعیت نزد مالی"] = test.soorat_vaziat_mali
                            
                        elif col.additional_attrs and test.additional_attrs:
                            for key in col.additional_attrs.keys():
                                if key == col1:
                                    row_dict[col.additional_attrs[key]] = test.additional_attrs[key]    
                                    sum += test.additional_attrs[col1]
                    sum += test.dardast_ejra + test.tahie_soorat_vaziat + test.soorat_vaziat_mali + test.soorat_vaziat_setad
                    row_dict[col.majmo_name] = sum
                        
            rows_data.append(row_dict)
        df = pd.DataFrame(rows_data)
        df.replace([np.inf, -np.inf , np.nan], None, inplace=True)
        
        
        
        for col in majmo_Cols:
            df.drop(col,axis=1,inplace=True)
        cols_for_sum = list(df.columns)
        cols_for_sum.remove("نام شهر")
        cols_for_sum.remove("شماره تست")
        cols_for_sum.remove("سال")
        cols_for_sum.remove("ماه")
        # grouped = df.groupby(group_cols, dropna=False).sum(numeric_only=True).reset_index()
        a = pd.DataFrame()
        
        for i , row in df.iterrows():
            jam:int  =0
            for col_t in collection_tests_refs:
                if row["شماره تست"] == col_t.test_num:
                    for col in df.columns:
                        if col in cols_for_sum:
                            if row[col] is not None:
                                jam += row[col]
                            else:
                                jam +=0
                    a[col_t.majmo_name]=jam
        new_df = pd.concat([df,a],axis=1)
        new_df.replace([np.inf, -np.inf , np.nan], None, inplace=True)
        g_cols = ["نام شهر" , "سال" , "ماه"]
        
        grouped = df.drop("شماره تست",axis=1).groupby(g_cols, dropna=False).sum().reset_index()
        
        
        k_cols = list(grouped.columns)
        for i in g_cols:
            k_cols.remove(i)

        grouped["کل دستور کارهای باز"] = grouped[k_cols].sum(axis=1, skipna=True)
        
        grouped.replace([np.inf, -np.inf , np.nan], None, inplace=True)
        print(grouped)
        return grouped.to_dict(orient="records")

                    



class test_type(BaseModel):
    city_name : str
    test_num : int
    year : int
    month : int
    dardast_ejra : int
    tahie_soorat_vaziat : int
    soorat_vaziat_setad : int
    soorat_vaziat_mali : int
    additional_attrs : Optional[Dict[str,int]]

@router.post("/tests/add")
def add_test(input : test_type):
    with Session1.begin() as con:
        try:
            new_test = tests(input.city_name,input.test_num,input.year,input.month,input.dardast_ejra, input.tahie_soorat_vaziat,input.soorat_vaziat_mali,input.soorat_vaziat_mali,input.additional_attrs)
            con.add(new_test)
            con.commit()
            return{"added"}
        except:
            return{"something went wrong"}

class edit_test_type(BaseModel):
    city_name : str
    test_num : int
    year : int
    month : int        
class edit_test_type_to_update(BaseModel):
    dardast_ejra : int
    tahie_soorat_vaziat : int
    soorat_vaziat_setad : int
    soorat_vaziat_mali : int
    additional_attrs : Optional[Dict[str,int]]

@router.put("/tests/edit")
def edit_test(input:edit_test_type , update : edit_test_type_to_update):
    with Session1.begin() as con:
        the_test = con.query(tests).where(and_(tests.city_name == input.city_name , tests.test_num == input.test_num,tests.year == input.year,tests.month == input.month)).first()
        if the_test:
            try:
                the_test.dardast_ejra =update.dardast_ejra
                the_test.tahie_soorat_vaziat = update.tahie_soorat_vaziat
                the_test.soorat_vaziat_mali = update.soorat_vaziat_mali
                the_test.soorat_vaziat_setad = update.soorat_vaziat_setad
                the_test.additional_attrs = update.additional_attrs
                con.commit()
                return {"edited"}
            except:
                return{"something went wrong"}
        else:
            return{"not found"}

@router.delete("/tests/delete")
def delete_test(input:edit_test_type ):
    with Session1.begin() as con:
        the_test = con.query(tests).where(and_(tests.city_name == input.city_name , tests.test_num == input.test_num,tests.year == input.year,tests.month == input.month)).first()
        if the_test:
            try:
                con.delete(the_test)
                con.commit()
                return {"deleted"}
            except:
                return{"something went wrong"}
        else:
            return{"not found"}
