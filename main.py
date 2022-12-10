import os,sys
from Ratings.config.mongo_db_connection import MongoDBClient
from Ratings.logger import logging
from Ratings.exception import RatingsException
from fastapi import FastAPI, File, UploadFile
from Ratings.constant.application import APP_HOST, APP_PORT
from uvicorn import run as app_run
from Ratings.entity.config_entity import TrainingPipelineConfig,DataIngestionConfig
from Ratings.pipeline.training_pipeline import TrainPipeline
import pandas as pd
from Ratings.utils.main_utils import read_yaml_file
from Ratings.constant.training_pipeline import SAVED_MODEL_DIR
from fastapi import FastAPI, File, UploadFile
from Ratings.constant.application import APP_HOST, APP_PORT
from starlette.responses import RedirectResponse
from uvicorn import run as app_run
from fastapi.responses import Response
from Ratings.ml.model.estimator import ModelResolver 
from Ratings.utils.main_utils import load_object
from fastapi.middleware.cors import CORSMiddleware
            #for testing exception
#def test_exception():
#    try:
 #       logging.info("dividing 1 by 0")
 #       x=1/0
 #   except Exception as e:
 #       raise RatingsException(e,sys)
 
           #for testing start_data_ingestion function
env_file_path=os.path.join(os.getcwd(),"env.yaml")

def set_env_variable(env_file_path):

    if os.getenv('MONGO_DB_URL',None) is None:
        env_config = read_yaml_file(env_file_path)
        os.environ['MONGO_DB_URL']=env_config['MONGO_DB_URL']


app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["authentication"])
async def index():
    return RedirectResponse(url="/docs")

@app.get("/train")
async def train_route():
    try:

        train_pipeline = TrainPipeline()
        if train_pipeline.is_pipeline_running:
            return Response("Training pipeline is already running.")
        train_pipeline.run_pipeline()
        return Response("Training successful !!")
    except Exception as e:
        return Response(f"Error Occurred! {e}")

@app.get("/predict")
async def predict_route():
    try:
        #get data from user csv file
        #conver csv file to dataframe
        df = None
        model_resolver = ModelResolver(model_dir=SAVED_MODEL_DIR)
        if not model_resolver.is_model_exists():
            return Response("Model is not available")
        
        best_model_path = model_resolver.get_best_model_path()
        model = load_object(file_path=best_model_path)
        y_pred = model.predict(df)
        df['predicted_column'] = y_pred
        #df['predicted_column'].replace(TargetValueMapping().reverse_mapping(),inplace=True)
        return df.to_html()
        #decide how to return file to user.
        
    except Exception as e:
        raise Response(f"Error Occured! {e}")

def main():
    try:
        set_env_variable(env_file_path)
        training_pipeline = TrainPipeline()
        training_pipeline.run_pipeline()
    except Exception as e:
            print(e)
            logging.exception(e)
if __name__ == "__main__":
    #training_pipeline=TrainPipeline()
    #training_pipeline.run_pipeline()
    app_run(app, host=APP_HOST, port=APP_PORT)

                  
    #for testing               
                  #for testing DataIngestionConfig
    #training_pipeline_config=TrainingPipelineConfig()
    #data_ingestion_config=DataIngestionConfig(training_pipeline_config=training_pipeline_config )
    #print(data_ingestion_config.__dict__)

             #for testing mongo_db_connection file
    # mongdb_client=MongoDBClient()
    #print(mongdb_client.database.list_collection_names())

             #for testing exception and logger#
    #try:
    #   test_exception()
    #except Exception as e:
    #   print(e)
    
