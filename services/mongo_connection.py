import pymongo
from pymongo import MongoClient


class Connection:

    def __init__(self, host='localhost', port='11223', user='jon', password='jonsnow',
                 database='jonsnow', collection='prisms'):
        try:
            client = MongoClient('mongodb://{usr}:{pwd}@{host}:{port}/'
                                 .format(host=host, port=port, usr=user, pwd=password))
            db = client[database]
            coll = db[collection]
        except:
            print("Database connection failed!")
            exit(1)


      # def write():
      #
      #
      #   val collection = this.connectMongoDB()
      #   val id = df.schema.fields.head.name
      #   val predictionValue = "prediction"
      #
      #   val values = df.rdd.map(x => (x.getAs[String](id).toInt, x.getAs[Double](predictionValue)))
      #     .map(x => MongoDBObject("gid" -> x._1, "aqi" -> x._2)).collect()
      #
      #   val mongoDBList = new MongoDBList()
      #   for (each <- values) mongoDBList += each
      #
      #   val insertAll = MongoDBObject("timestamp"-> new DateTime(time.getTime).toDate, "data" -> mongoDBList)
      #   collection.insert(insertAll)
      #   println(s"Insert $time Successfully!!!")


