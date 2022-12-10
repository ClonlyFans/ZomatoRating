import sys
from typing import Optional
import pytz
import dateutil
import numpy as np
import pandas as pd
import json
from Ratings.config.mongo_db_connection import MongoDBClient
from Ratings.constant.database import DATABASE_NAME
from Ratings.exception import RatingsException
from sklearn.preprocessing import LabelEncoder
from Ratings.logger import logging

class RatingData:
    """
    This class help to export entire mongo db record as pandas dataframe
    """

    def __init__(self):
        """
        """
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)

        except Exception as e:
            raise RatingsException(e, sys)
    
    

    def save_csv_file(self,file_path ,collection_name: str, database_name: Optional[str] = None):
        try:
            data_frame=pd.read_csv(file_path)
            data_frame.reset_index(drop=True, inplace=True)
            records = list(json.loads(data_frame.T.to_json()).values())
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            raise RatingsException(e, sys)


    def export_collection_as_dataframe(
        self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        try:
            """
            export entire collectin as dataframe:
            return pd.DataFrame of collection
            """
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]
            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            logging.info("dropped the column _id created by mongoDB database")
                
            df.replace({"na":np.nan}, inplace=True)

            df.replace({np.nan:0}, inplace=True)
            df.replace({'':0}, inplace=True)
            logging.info(df['approx_cost(for two people)'].unique().__str__())
            logging.info("Replaced all the 'na' recorded by 'nan' and 'nan' record by 0")

            df=df.rename(columns={"approx_cost(for two people)":"cost"})
            df["cost"]=df["cost"].astype(str)
            df['cost'] = df['cost'].str.replace(',', '')
            df['cost'] = df['cost'].astype(int) 
        

            logging.info("processed the approx_cost(for two people) column and converted the cost column to float")
        

            df["rate"]=df["rate"].astype(str)
            df["rate"]=df["rate"].apply(lambda x: x.replace("/5", "")).apply(lambda x: x.strip())
            df["rate"]=df["rate"].apply(lambda x: x.replace("NEW", str(np.nan)).replace("-", str(np.nan)))
            df["rate"]=df["rate"].replace("nan", 0.0).astype(float)
            df["rate"]=round(df["rate"])
            df['rate'] = df['rate'].astype('int')
        
    
        
            #changing reviews with score less than 3 to be positive and vice-versa
            df['rate'] = df['rate'].map(lambda x : 1 if(x > 3) else 0 )
            logging.info("processed the rate column and converted to float")      
            
            logging.info("Label encoding starts")  

            df[['online_order', 'book_table','listed_in(type)','listed_in(city)']] = df[['online_order', 'book_table','listed_in(type)','listed_in(city)']].apply(LabelEncoder().fit_transform)

            logging.info("Label encoded") 
            return df

        except Exception as e:
            raise RatingsException(e, sys)