import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data in S3 buckets to staging tables.
    Input tables: s3://udacity-dend/song_data, s3://udacity-dend/log_data, s3://udacity-dend/log_json_path.json
    Output tables: staging_events, staging_songs
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform staging tables to fact and dimensional tables to build a star schema model optimized for queries on song play analysis.
    Input tables: staging_events, staging_songs
    Output tables: songplays, users, songs, artists, time
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Initialize ETL pipeline to load data from S3 to staging tables on AWS Redshift and execute SQL statements that create the analytics tables from staging tables.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()