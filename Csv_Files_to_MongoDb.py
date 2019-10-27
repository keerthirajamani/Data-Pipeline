import os
import pandas as pd
import pymongo

collections=[]
collection_list=[]
def MongoDB():
    Uploading=0;
    loc = "/home/jetfire/DataSet/"  #Takes all the csv files in this parent directory
    files = []
    for r, d, f in os.walk(loc):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))
                collections.append(file)
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb= myclient["file_Upload_db"]
    Strirnd=str(myclient)
    if Strirnd.__contains__('connect=True)'):
        print("Connected to ",Strirnd)
        Uploading=1;
    #print(Strirnd)
    #print(mydb)
    print("Uploading files to MongoDb...")
    if Uploading==1:
         for f in files:
             my_File=pd.read_csv(f,quotechar='"',delimiter=",",doublequote=True,skipfooter=0)
             S=f.replace(".","_").split("/")[-1]
             #print(S)
             mycoll=mydb[S] #creating table with filename.csv
             mycoll.insert_many(my_File.to_dict(orient='records')) #inserting records into table, once they are converted into dictionary with records oriented
    print("Insertion Completed")
if __name__ == "__main__":
    MongoDB()
