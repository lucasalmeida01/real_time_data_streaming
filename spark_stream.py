import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configuração do logger
logging.basicConfig(level=logging.INFO)

# Função para criar a keyspace no Cassandra
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created successfully!")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")

# Função para criar a tabela no Cassandra
def create_table(session):
    try:
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
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
        logging.info("Table 'created_users' created successfully!")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

# Função para inserir dados no Cassandra
def insert_data(session, **kwargs):
    logging.info("Inserting data...")
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            kwargs.get('id'), kwargs.get('first_name'), kwargs.get('last_name'), kwargs.get('gender'),
            kwargs.get('address'), kwargs.get('post_code'), kwargs.get('email'), kwargs.get('username'),
            kwargs.get('dob'), kwargs.get('registered_date'), kwargs.get('phone'), kwargs.get('picture')
        ))
        logging.info(f"Data inserted for {kwargs.get('first_name')} {kwargs.get('last_name')}")
    except Exception as e:
        logging.error(f"Could not insert data: {e}")

# Função para criar a conexão com o Spark
def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                                           "org.apache.kafka:kafka-clients:3.3.2") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session: {e}")
        return None

# Função para conectar ao Kafka e criar o DataFrame
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        if spark_df.isStreaming:
            logging.info("Connection to Kafka established.")
        else:
            logging.warning("DataFrame is not streaming.")

        return spark_df
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return None

# Função para criar a conexão com o Cassandra
def create_cassandra_connection():
    logging.info("Connecting to Cassandra cluster on 'cassandra_db'")
    try:
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logging.info("Connection to Cassandra established successfully.")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

# Função para criar o DataFrame de seleção a partir do Kafka
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    try:
        sel_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
        logging.info("Schema applied successfully to Kafka data.")
        return sel_df
    except Exception as e:
        logging.error(f"Error applying schema to Kafka data: {e}")
        return None

if __name__ == "__main__":
    # Criar conexão com Spark
    spark_conn = create_spark_connection()

    if spark_conn:
        # Conectar ao Kafka e obter o DataFrame
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            # Estruturar o DataFrame para inserir no Cassandra
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                logging.info("Starting streaming to Cassandra...")

                try:
                    streaming_query = (selection_df.writeStream
                                       .format("org.apache.spark.sql.cassandra")
                                       .option('checkpointLocation', '/tmp/checkpoint')
                                       .option('keyspace', 'spark_streams')
                                       .option('table', 'created_users')
                                       .start())

                    streaming_query.awaitTermination()
                except Exception as e:
                    logging.error(f"Error during streaming: {e}")
