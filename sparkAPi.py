import requests
import json
import sys
from pyspark.sql.types import BooleanType
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
from pyspark.sql.functions import count, lit, sum
import parser

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SPARK -> GET -> API") \
        .getOrCreate()

    def getDataFromApi():
        url = "http://144.202.17.134:8000/obtenerData"
        response = requests.get(url)
        return response
    

    data = getDataFromApi()
    json_rdd = spark.sparkContext.parallelize([data.text])
    df = spark.read.json(json_rdd)
    #result = df.select("estado").show()
    

    print("\n")
    print("TOTAL DE ESTADOS DEL SENSOR : \n")
    print("RESULTADOS DE : filtrado1,filtrado2 Y filtrado3")
    filtrado1 = df.select("estado").filter(df.estado == 'derecha').show()
    filtrado2 = df.select("estado").filter(df.estado == 'izquierda').show()
    filtrado3 = df.select("estado").filter(df.estado == 'apagado').show()
    
    filtradoD = json_rdd.flatMap(lambda json_rdd: json_rdd.split("derecha")).countByValue() #TOTAL REGISTROS "DERECHA"
    filtradoI = json_rdd.flatMap(lambda json_rdd: json_rdd.split("izquierda")).countByValue() #TOTAL REGISTROS IZQUIERDA
    filtradoA = json_rdd.flatMap(lambda json_rdd: json_rdd.split("apagado")).countByValue() #TOTAL REGISTROS APAGADO
    filtradoTOTAL = json_rdd.flatMap(lambda json_rdd: json_rdd.split("estado")).countByValue() #TOTAL REGISTROS APAGADO

    print("Total DERECHA",len(filtradoD))
    print("Total IZQUIERDA",len(filtradoI))
    print("Total APAGADO",len(filtradoA))
    print("TOTAL: ",len(filtradoTOTAL))
    print("\n")
    print(filtrado1)
    print(filtrado2)
    print(filtrado1)

    print("AQUI \n")
    filtradoA1 = df.select("estado").filter(df.estado == 'derecha')
    filtradoB1 = df.select("estado").filter(df.estado == 'izquierda')
    filtradoC1 = df.select("estado").filter(df.estado == 'apagado')
    filtradoA1.show()
    filtradoB1.show()
    filtradoC1.show()
    print(filtradoA1.count())
    print(filtradoB1.count())
    print(filtradoC1.count())
    r1 = filtradoA1.count()
    r2 = filtradoB1.count()
    r3 = filtradoC1.count()
    total = r1 + r2 + r3
    totalD = (r1 * 100 ) / total
    totalI = (r2 * 100) / total
    totalA = (r3 * 100 ) / total
    print("Total de registros: ",total)
    print("Total de registros DERECHA : ","%",totalD)
    print("Total de registros IZQUIERDA : ","%",totalI)
    print("Total de registros APAGADO : ","%",totalA)
    spark.stop()