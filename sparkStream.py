import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def createKeyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def createTable(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")

def insertData(session, **kwargs):
    print("Inserting data...")

    userId = kwargs.get('id')
    firstName = kwargs.get('first_name')
    lastName = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postCode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registeredDate = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (userId, firstName, lastName, gender, address,
              postCode, email, username, dob, registeredDate, phone, picture))
        logging.info(f"Data inserted for {firstName} {lastName}")

    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def createSparkConnection():
    sparkConn = None

    try:
        sparkConn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        sparkConn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")

    return sparkConn

def connectToKafka(sparkConn):
    sparkDf = None
    try:
        sparkDf = sparkConn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")

    return sparkDf

def createCassandraConnection():
    try:
        cluster = Cluster(['localhost'])
        casSession = cluster.connect()

        return casSession
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def createSelectionDfFromKafka(sparkDf):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = sparkDf.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    sparkConn = createSparkConnection()

    if sparkConn is not None:
        sparkDf = connectToKafka(sparkConn)
        selectionDf = createSelectionDfFromKafka(sparkDf)
        session = createCassandraConnection()

        if session is not None:
            createKeyspace(session)
            createTable(session)

            logging.info("Streaming is being started...")

            streamingQuery = (selectionDf.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streamingQuery.awaitTermination()
