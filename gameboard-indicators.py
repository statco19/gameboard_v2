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

# 챔피언별 지표
# 딜량
DF1 = df.groupBy('championName').agg(F.mean('totalDamageDealt').alias('avg_totalDamageDealt'))
# 포탑에 가한 딜량
DF2 = df.groupBy('championName').agg(F.mean('damageDealtToBuildings').alias('avg_'))
# 킬
DF3 = df.groupBy('championName').agg(F.mean('kills').alias('avg_kills'))
# 획득 골드
DF4 = df.groupBy('championName').agg(F.mean('goldEarned').alias('avg_goldEarned'))
# 시야 점수
DF5 = df.groupBy('championName').agg(F.mean('visionScore').alias('avg_visionScore'))
# 펜타킬 수
DF6 = df.groupBy('championName').agg(F.mean('pentaKills').alias('avg_pentaKills'))
# 오브젝트 스틸 개수
DF7 = df.groupBy('championName').agg(F.mean('objectivesStolen').alias('avg_objectivesStolen'))
# cs개수
DF8 = df.groupBy('championName').agg(F.mean('totalMinionsKilled').alias('avg_totalMinionsKilled'))

for i in range(1, 9):
    globals()['resJson'+str(i)] = globals()['DF'+str(i)].toJSON()


# elasticsearch로 전송
from elasticsearch import Elasticsearch

# method sending json to ES
def insertData1(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="totaldamage_avg", doc_type="_doc", body=doc)


def insertData2(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="buildingsdamage_avg", doc_type="_doc", body=doc)


def insertData3(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="kill_avg", doc_type="_doc", body=doc)


def insertData4(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="gold_avg", doc_type="_doc", body=doc)


def insertData5(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="visionscore_avg", doc_type="_doc", body=doc)


def insertData6(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="pentakill_avg", doc_type="_doc", body=doc)


def insertData7(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="objectstolen_avg", doc_type="_doc", body=doc)


def insertData8(doc):
    es = Elasticsearch('localhost:9200')
    es.index(index="minionkilled_avg", doc_type="_doc", body=doc)


es = Elasticsearch('localhost:9200')
for doc in resJson1.collect():
    insertData1(doc)

for doc in resJson2.collect():
    insertData2(doc)

for doc in resJson3.collect():
    insertData3(doc)

for doc in resJson4.collect():
    insertData4(doc)

for doc in resJson5.collect():
    insertData5(doc)

for doc in resJson6.collect():
    insertData6(doc)

for doc in resJson7.collect():
    insertData7(doc)

for doc in resJson8.collect():
    insertData8(doc)
