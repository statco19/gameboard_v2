import io
import json
import boto3
from pyspark.sql import SparkSession
from pyspark import SQLContext, SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import functions as F

accessKey = '****'
secretKey = '****'
bucket_name = '****'
region = '****'

#s3 client 생성
s3 = boto3.client('s3', aws_access_key_id = accessKey, aws_secret_access_key = secretKey, region_name = region)

#원하는 bucket과 하위 경로에 있는 object list
obj_list = s3.list_objects(Bucket = bucket_name)

#object list의 Contents를 가져옴
contents_list = obj_list['Contents']

#Content list 출력
file_list=[]
for content in contents_list:
    key = content['Key']
    file_list.append(key)

s3_client = boto3.client(service_name="s3",
                         aws_access_key_id=accessKey,
                         aws_secret_access_key=secretKey)

total = 0

for i,key_json in enumerate(file_list):
    obj = s3_client.get_object(Bucket=bucket_name, Key=key_json)
    data = json.load((io.BytesIO(obj["Body"].read())))
    teams = data["participants"]
    rdd = sc.parallelize(list(teams.items()))
    pre = rdd.map(lambda x: (dict([x])))\
        .map(lambda x: list(x.values())[0])
    res = pre.map(lambda x: x[0])
    df_tmp = res.toDF()
    for k in range(1, 10):
        tmp = pre.map(lambda x: x[k])
        tmp = tmp.toDF()
        df_tmp = df_tmp.union(tmp)
    total += df_tmp.count()
    if i == 0:
        df = df_tmp
    else:
        df = df.union(df_tmp) # df

#승률
DF_win = df.select(col('championName'), col('win').cast("integer")).groupBy('championName').agg(F.sum('win').alias('win_count'))
#픽률
DF_pick = df.select(col('championName'), col('win').cast("integer")).groupBy('championName').agg(F.count('win').alias('pick_count'))

#json으로 변환
resJson_win = DF_win.toJSON()
resJson_pick = DF_pick.toJSON()

# elasticsearch로 전송
from elasticsearch import Elasticsearch

# method sending json to ES
def insertData_win(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="win_count, doc_type="_doc", body=doc)


def insertData_pick(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="ban_count", doc_type="_doc", body=doc)

es = Elasticsearch('localhost:9200')
for doc in resJson_win.collect():
    insertData_win(doc)

for doc in resJson_pick.collect():
    insertData_pick(doc)

