import os
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import subprocess
from sqlalchemy import create_engine
import numpy as np

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'listings-airbnb',
    'user': 'postgres',
    'password': 'your_password',
    'port': '5432'
}

# 数据目录
DATA_DIR = "/Users/jia/Library/CloudStorage/OneDrive-个人/projects/gdssa-airbnb-geotext-feature/data-airbnb"

def setup_database():
    """设置数据库和PostGIS扩展"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                # 创建 PostGIS 扩展
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                print("PostGIS extension created successfully")
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        raise

def get_column_types(sample_df):
    """分析DataFrame的列类型并返回对应的PostgreSQL类型"""
    pg_type_map = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'category': 'TEXT'
    }
    
    column_types = {}
    for column in sample_df.columns:
        if column in ['first_review', 'last_review', 'host_since']:
            column_types[column] = 'TIMESTAMP'
        else:
            dtype = str(sample_df[column].dtype)
            if dtype in pg_type_map:
                column_types[column] = pg_type_map[dtype]
            else:
                column_types[column] = 'TEXT'
    
    return column_types

def create_table_sql(column_types):
    """生成创建表的SQL语句，添加PostGIS几何字段"""
    columns = [f'"{col}" {dtype}' for col, dtype in column_types.items()]
    columns.append('"city" TEXT')
    columns.append('geom geometry(Point, 4326)')  # 添加PostGIS几何字段
    
    columns_str = ',\n        '.join(columns)
    
    return f"""
    DROP TABLE IF EXISTS listings;
    CREATE TABLE listings (
        {columns_str}
    );
    """

def create_indices_sql():
    """生成创建索引的SQL语句，包括空间索引"""
    return """
    CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city);
    CREATE INDEX IF NOT EXISTS idx_listings_host_id ON listings(host_id);
    CREATE INDEX IF NOT EXISTS idx_listings_first_review ON listings(first_review);
    CREATE INDEX IF NOT EXISTS idx_listings_geom ON listings USING GIST(geom);
    """

def analyze_data_structure():
    """分析数据结构并创建表"""
    # 找到第一个可用的CSV文件来分析结构
    for city in os.listdir(DATA_DIR):
        city_path = os.path.join(DATA_DIR, city)
        if os.path.isdir(city_path):
            listings_file = os.path.join(city_path, "listings.csv.gz")
            if os.path.exists(listings_file):
                print(f"Analyzing structure using {city}...")
                df = pd.read_csv(listings_file, nrows=1000)  # 只读取1000行用于分析
                column_types = get_column_types(df)
                return column_types
    
    raise Exception("No CSV files found for analysis")

def import_data():
    """导入数据并创建空间数据"""
    try:
        # 设置数据库和PostGIS
        setup_database()
        
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # 分析数据结构并创建表
        print("Analyzing data structure...")
        column_types = analyze_data_structure()
        
        # 创建表
        print("Creating table...")
        cur.execute(create_table_sql(column_types))
        
        # 遍历所有城市文件夹并导入数据
        for city in os.listdir(DATA_DIR):
            city_path = os.path.join(DATA_DIR, city)
            if os.path.isdir(city_path):
                listings_file = os.path.join(city_path, "listings.csv.gz")
                if os.path.exists(listings_file):
                    print(f"Importing {city}...")
                    
                    # 构建COPY命令
                    columns = list(column_types.keys())
                    columns_str = ', '.join(f'"{col}"' for col in columns)
                    
                    # 使用COPY导入基础数据
                    copy_command = f"""\\COPY listings({columns_str}) FROM STDIN WITH (FORMAT csv, HEADER true)"""
                    
                    process = subprocess.Popen(
                        ['gunzip', '-c', listings_file],
                        stdout=subprocess.PIPE
                    )
                    
                    psql_env = os.environ.copy()
                    psql_env['PGPASSWORD'] = DB_CONFIG['password']
                    
                    psql_process = subprocess.Popen(
                        [
                            'psql',
                            '-h', DB_CONFIG['host'],
                            '-U', DB_CONFIG['user'],
                            '-d', DB_CONFIG['database'],
                            '-c', copy_command
                        ],
                        stdin=process.stdout,
                        env=psql_env
                    )
                    
                    psql_process.wait()
                    
                    # 更新城市名和几何数据
                    print(f"Updating spatial data for {city}...")
                    cur.execute("""
                        UPDATE listings 
                        SET 
                            city = %s,
                            geom = ST_SetSRID(ST_MakePoint(
                                CAST(longitude AS FLOAT), 
                                CAST(latitude AS FLOAT)
                            ), 4326)
                        WHERE city IS NULL
                    """, (city,))
                    
                    print(f"Completed {city}")
        
        # 创建索引
        print("Creating indices...")
        cur.execute(create_indices_sql())
        
        print("Data import completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def verify_import():
    """验证导入的数据"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 检查每个城市的记录数
        cur.execute("SELECT city, COUNT(*) FROM listings GROUP BY city")
        results = cur.fetchall()
        
        print("\nImport verification:")
        print("City counts:")
        for city, count in results:
            print(f"{city}: {count} records")
        
        # 检查总记录数
        cur.execute("SELECT COUNT(*) FROM listings")
        total_count = cur.fetchone()[0]
        print(f"\nTotal records: {total_count}")
        
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Starting data import process...")
    import_data()
    verify_import() 