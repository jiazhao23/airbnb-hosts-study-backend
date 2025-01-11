from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import h3
from typing import List, Dict
from collections import Counter
import json

app = FastAPI()

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'listings-airbnb',
    'user': 'postgres',
    'password': 'your_password',
    'port': '5432'
}

def get_db_connection():
    """获取数据库连接"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

@app.get("/cities")
async def get_cities():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT city FROM listings ORDER BY city")
                cities = [row['city'] for row in cur.fetchall()]
                return {"cities": cities}
    except Exception as e:
        print(f"Error in get_cities: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/city/{city_name}")
async def get_city_listings(city_name: str):
    try:
        print(f"Processing request for city: {city_name}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 获取时间窗口，直接使用 TIMESTAMP 类型的列
                print("Executing time window query...")
                cur.execute("""
                    SELECT 
                        MIN(first_review) as earliest,
                        MAX(first_review) as latest
                    FROM listings 
                    WHERE city = %s 
                    AND first_review IS NOT NULL
                """, (city_name,))
                time_window = cur.fetchone()
                print(f"Time window result: {time_window}")
                
                # 获取中心坐标
                print("Executing center coordinates query...")
                cur.execute("""
                    SELECT 
                        AVG(CAST(latitude AS FLOAT)) as lat,
                        AVG(CAST(longitude AS FLOAT)) as lng
                    FROM listings 
                    WHERE city = %s 
                    AND latitude IS NOT NULL 
                    AND longitude IS NOT NULL
                """, (city_name,))
                center = cur.fetchone()
                print(f"Center coordinates result: {center}")
                
                if not center or not time_window:
                    print(f"No data found for city: {city_name}")
                    raise HTTPException(status_code=404, detail=f"City not found: {city_name}")
                
                # 检查数据有效性
                if center['lat'] is None or center['lng'] is None:
                    print(f"Invalid coordinates for city: {city_name}")
                    raise HTTPException(status_code=500, detail=f"Invalid coordinates for city: {city_name}")
                
                result = {
                    "center": {
                        "latitude": float(center['lat']),
                        "longitude": float(center['lng'])
                    },
                    "time_window": {
                        "earliest": time_window['earliest'].strftime('%Y-%m-%d') if time_window['earliest'] else None,
                        "latest": time_window['latest'].strftime('%Y-%m-%d') if time_window['latest'] else None
                    }
                }
                print(f"Returning result: {result}")
                return result
                
    except HTTPException as he:
        print(f"HTTP Exception: {str(he)}")
        raise he
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/city/{city_name}/host_ranking")
async def get_host_ranking(city_name: str, time_point: str):
    try:
        target_date = datetime.strptime(time_point, "%Y-%m")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 获取符合条件的房源数据
                cur.execute("""
                    WITH host_listings AS (
                        SELECT 
                            host_id,
                            COUNT(*) as listing_count
                        FROM listings
                        WHERE city = %s 
                        AND first_review <= %s
                        GROUP BY host_id
                        ORDER BY listing_count DESC
                    )
                    SELECT * FROM host_listings
                """, (city_name, target_date))
                
                results = cur.fetchall()
                if not results:
                    return {
                        "host_categories": {},
                        "total_hosts": 0,
                        "total_listings": 0
                    }
                
                # 转换为 DataFrame 进行分类
                df = pd.DataFrame(results)
                
                # 分类处理
                class_5 = df[df['listing_count'] == 1]
                class_4 = df[df['listing_count'] == 2]
                remaining_hosts = df[df['listing_count'] > 2]
                
                if len(remaining_hosts) > 0:
                    p5_index = max(1, int(len(remaining_hosts) * 0.05))
                    p15_index = max(p5_index + 1, int(len(remaining_hosts) * 0.15))
                    
                    class_1 = remaining_hosts.iloc[:p5_index]
                    class_2 = remaining_hosts.iloc[p5_index:p15_index]
                    class_3 = remaining_hosts.iloc[p15_index:]
                else:
                    class_1 = pd.DataFrame()
                    class_2 = pd.DataFrame()
                    class_3 = pd.DataFrame()
                
                def get_category_info(df):
                    if len(df) == 0:
                        return {
                            "range": None,
                            "count": 0,
                            "host_ids": []
                        }
                    return {
                        "range": {
                            "min": int(df['listing_count'].min()),
                            "max": int(df['listing_count'].max())
                        },
                        "count": len(df),
                        "host_ids": [str(id) for id in df['host_id']]
                    }
                
                host_categories = {
                    "highly_commercial": get_category_info(class_1),
                    "commercial": get_category_info(class_2),
                    "semi_commercial": get_category_info(class_3),
                    "dual_host": get_category_info(class_4),
                    "single_host": get_category_info(class_5)
                }
                
                return {
                    "host_categories": host_categories,
                    "total_hosts": len(df),
                    "total_listings": int(df['listing_count'].sum())
                }
                
    except ValueError as ve:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid time format: {str(ve)}. Please use YYYY-MM format."
        )
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/city/{city_name}/listings_by_categories")
async def get_listings_by_categories(
    city_name: str, 
    time_point: str, 
    categories: str
):
    try:
        target_date = datetime.strptime(time_point, "%Y-%m")
        selected_categories = categories.split(',')
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 获取房东分类
                cur.execute("""
                    WITH host_listings AS (
                        SELECT 
                            host_id,
                            COUNT(*) as listing_count
                        FROM listings
                        WHERE city = %s 
                        AND first_review <= %s
                        GROUP BY host_id
                    )
                    SELECT * FROM host_listings
                    ORDER BY listing_count DESC
                """, (city_name, target_date))
                
                results = cur.fetchall()
                if not results:
                    return {"listings": [], "total_listings": 0}
                
                # 分类房东
                df = pd.DataFrame(results)
                single_hosts = set(df[df['listing_count'] == 1]['host_id'])
                dual_hosts = set(df[df['listing_count'] == 2]['host_id'])
                multi_hosts = df[df['listing_count'] > 2]
                
                selected_hosts = set()
                if len(multi_hosts) > 0:
                    p5_count = max(1, int(len(multi_hosts) * 0.05))
                    p15_count = max(p5_count + 1, int(len(multi_hosts) * 0.15))
                    
                    if 'highly_commercial' in selected_categories:
                        selected_hosts.update(multi_hosts.iloc[:p5_count]['host_id'])
                    if 'commercial' in selected_categories:
                        selected_hosts.update(multi_hosts.iloc[p5_count:p15_count]['host_id'])
                    if 'semi_commercial' in selected_categories:
                        selected_hosts.update(multi_hosts.iloc[p15_count:]['host_id'])
                
                if 'dual_host' in selected_categories:
                    selected_hosts.update(dual_hosts)
                if 'single_host' in selected_categories:
                    selected_hosts.update(single_hosts)
                
                if not selected_hosts:
                    return {"listings": [], "total_listings": 0}
                
                # 获取选中房东的房源
                host_ids = tuple(selected_hosts)
                cur.execute("""
                    SELECT 
                        host_id,
                        latitude,
                        longitude,
                        name,
                        price
                    FROM listings
                    WHERE city = %s 
                    AND first_review <= %s
                    AND host_id = ANY(%s)
                    AND latitude IS NOT NULL
                    AND longitude IS NOT NULL
                """, (city_name, target_date, list(host_ids)))
                
                listings = cur.fetchall()
                return {
                    "listings": listings,
                    "total_listings": len(listings)
                }
                
    except ValueError as ve:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid time format: {str(ve)}. Please use YYYY-MM format."
        )
    except Exception as e:
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/city/{city_name}/hexgrid")
async def get_city_hexgrid(
    city_name: str,
    time_point: str = None,
    categories: str = None
):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 构建基础查询
                if time_point and categories:
                    target_date = datetime.strptime(time_point, "%Y-%m")
                    selected_categories = categories.split(',')
                    
                    # 获取符合条件的房东
                    cur.execute("""
                        WITH host_listings AS (
                            SELECT 
                                host_id,
                                COUNT(*) as listing_count
                            FROM listings
                            WHERE city = %s 
                            AND first_review <= %s
                            GROUP BY host_id
                        )
                        SELECT host_id, listing_count
                        FROM host_listings
                        ORDER BY listing_count DESC
                    """, (city_name, target_date))
                    
                    results = cur.fetchall()
                    if results:
                        df = pd.DataFrame(results)
                        selected_hosts = set()
                        
                        # 分类并选择房东
                        single_hosts = set(df[df['listing_count'] == 1]['host_id'])
                        dual_hosts = set(df[df['listing_count'] == 2]['host_id'])
                        multi_hosts = df[df['listing_count'] > 2]
                        
                        if len(multi_hosts) > 0:
                            p5_count = max(1, int(len(multi_hosts) * 0.05))
                            p15_count = max(p5_count + 1, int(len(multi_hosts) * 0.15))
                            
                            if 'highly_commercial' in selected_categories:
                                selected_hosts.update(multi_hosts.iloc[:p5_count]['host_id'])
                            if 'commercial' in selected_categories:
                                selected_hosts.update(multi_hosts.iloc[p5_count:p15_count]['host_id'])
                            if 'semi_commercial' in selected_categories:
                                selected_hosts.update(multi_hosts.iloc[p15_count:]['host_id'])
                        
                        if 'dual_host' in selected_categories:
                            selected_hosts.update(dual_hosts)
                        if 'single_host' in selected_categories:
                            selected_hosts.update(single_hosts)
                        
                        # 使用PostGIS获取坐标
                        if selected_hosts:
                            cur.execute("""
                                SELECT 
                                    ST_Y(geom) as latitude,
                                    ST_X(geom) as longitude
                                FROM listings
                                WHERE city = %s 
                                AND host_id = ANY(%s)
                                AND geom IS NOT NULL
                            """, (city_name, list(selected_hosts)))
                    else:
                        # 不带筛选的查询
                        cur.execute("""
                            SELECT 
                                ST_Y(geom) as latitude,
                                ST_X(geom) as longitude
                            FROM listings
                            WHERE city = %s
                            AND geom IS NOT NULL
                        """, (city_name,))
                    
                    results = cur.fetchall()
                    
                    if not results:
                        raise HTTPException(status_code=500, detail="No valid coordinates found")
                    
                    # 获取边界
                    cur.execute("""
                        SELECT 
                            ST_YMin(ST_Collect(geom)) as min_lat,
                            ST_YMax(ST_Collect(geom)) as max_lat,
                            ST_XMin(ST_Collect(geom)) as min_lng,
                            ST_XMax(ST_Collect(geom)) as max_lng
                        FROM listings
                        WHERE city = %s
                        AND geom IS NOT NULL
                    """, (city_name,))
                    
                    bounds_result = cur.fetchone()
                    
                    # 使用H3生成六边形网格
                    coords_df = pd.DataFrame(results)
                    resolution = 9
                    hex_ids = [
                        h3.latlng_to_cell(lat, lng, resolution)
                        for lat, lng in coords_df[['latitude', 'longitude']].values
                    ]
                    
                    # 计算每个六边形内的点数
                    hex_counts = Counter(hex_ids)
                    
                    # 生成六边形边界
                    hex_boundaries = []
                    for hex_id, count in hex_counts.items():
                        boundary = h3.cell_to_boundary(hex_id)
                        center = h3.cell_to_latlng(hex_id)
                        
                        hex_boundaries.append({
                            'id': str(hex_id),
                            'boundary': [list(point) for point in boundary],
                            'center': list(center),
                            'points_count': count
                        })
                    
                    bounds = {
                        'min_lat': float(bounds_result['min_lat']),
                        'max_lat': float(bounds_result['max_lat']),
                        'min_lng': float(bounds_result['min_lng']),
                        'max_lng': float(bounds_result['max_lng'])
                    }
                    
                    return {
                        'hexagons': hex_boundaries,
                        'bounds': bounds,
                        'total_hexagons': len(hex_counts),
                        'total_points': len(coords_df)
                    }
                    
    except Exception as e:
        print(f"Error generating hexgrid: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/city/{city_name}/yearly_stats")
async def get_yearly_stats(city_name: str):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 获取所有年份的数据
                cur.execute("""
                    WITH yearly_data AS (
                        SELECT 
                            host_id,
                            EXTRACT(YEAR FROM first_review) as year,
                            COUNT(*) as listing_count
                        FROM listings
                        WHERE city = %s 
                        AND first_review IS NOT NULL
                        GROUP BY host_id, EXTRACT(YEAR FROM first_review)
                    )
                    SELECT year, host_id, listing_count
                    FROM yearly_data
                    ORDER BY year, listing_count DESC
                """, (city_name,))
                
                results = cur.fetchall()
                if not results:
                    return {
                        "yearly_stats": {},
                        "year_range": {
                            "start": None,
                            "end": None
                        }
                    }
                
                # 按年份组织数据
                yearly_stats = {}
                df = pd.DataFrame(results)
                min_year = int(df['year'].min())
                max_year = int(df['year'].max())
                
                # 对每一年进行统计（除了第一年）
                for year in range(min_year + 1, max_year + 1):
                    year_df = df[df['year'] <= year].groupby('host_id')['listing_count'].sum().reset_index()
                    year_df = year_df.sort_values('listing_count', ascending=False)
                    
                    # 单房源和双房源房东
                    single_hosts = year_df[year_df['listing_count'] == 1]
                    dual_hosts = year_df[year_df['listing_count'] == 2]
                    multi_hosts = year_df[year_df['listing_count'] > 2]
                    
                    stats = {
                        "thresholds": {
                            "single_host": {"min": 1, "max": 1},
                            "dual_host": {"min": 2, "max": 2}
                        },
                        "counts": {
                            "single_host": len(single_hosts),
                            "dual_host": len(dual_hosts)
                        }
                    }
                    
                    # 处理多房源房东
                    if len(multi_hosts) > 0:
                        p5_count = max(1, int(len(multi_hosts) * 0.05))
                        p15_count = max(p5_count + 1, int(len(multi_hosts) * 0.15))
                        
                        highly_commercial = multi_hosts.iloc[:p5_count]
                        commercial = multi_hosts.iloc[p5_count:p15_count]
                        semi_commercial = multi_hosts.iloc[p15_count:]
                        
                        stats["thresholds"].update({
                            "highly_commercial": {
                                "min": int(highly_commercial['listing_count'].min()) if len(highly_commercial) > 0 else None,
                                "max": int(highly_commercial['listing_count'].max()) if len(highly_commercial) > 0 else None
                            },
                            "commercial": {
                                "min": int(commercial['listing_count'].min()) if len(commercial) > 0 else None,
                                "max": int(commercial['listing_count'].max()) if len(commercial) > 0 else None
                            },
                            "semi_commercial": {
                                "min": int(semi_commercial['listing_count'].min()) if len(semi_commercial) > 0 else None,
                                "max": int(semi_commercial['listing_count'].max()) if len(semi_commercial) > 0 else None
                            }
                        })
                        
                        stats["counts"].update({
                            "highly_commercial": len(highly_commercial),
                            "commercial": len(commercial),
                            "semi_commercial": len(semi_commercial)
                        })
                    else:
                        stats["thresholds"].update({
                            "highly_commercial": {"min": None, "max": None},
                            "commercial": {"min": None, "max": None},
                            "semi_commercial": {"min": None, "max": None}
                        })
                        stats["counts"].update({
                            "highly_commercial": 0,
                            "commercial": 0,
                            "semi_commercial": 0
                        })
                    
                    # 计算房东百分比
                    total_hosts = sum(stats["counts"].values())
                    stats["percentages"] = {
                        category: round(count / total_hosts * 100, 2)
                        for category, count in stats["counts"].items()
                    }
                    
                    # 计算房源数量
                    stats["listing_counts"] = {
                        "single_host": len(single_hosts),
                        "dual_host": len(dual_hosts) * 2
                    }
                    
                    if len(multi_hosts) > 0:
                        stats["listing_counts"].update({
                            "highly_commercial": int(highly_commercial['listing_count'].sum()) if len(highly_commercial) > 0 else 0,
                            "commercial": int(commercial['listing_count'].sum()) if len(commercial) > 0 else 0,
                            "semi_commercial": int(semi_commercial['listing_count'].sum()) if len(semi_commercial) > 0 else 0
                        })
                    else:
                        stats["listing_counts"].update({
                            "highly_commercial": 0,
                            "commercial": 0,
                            "semi_commercial": 0
                        })
                    
                    # 计算房源百分比
                    total_listings = sum(stats["listing_counts"].values())
                    stats["listing_percentages"] = {
                        category: round(count / total_listings * 100, 2)
                        for category, count in stats["listing_counts"].items()
                    }
                    
                    yearly_stats[str(year)] = stats
                
                return {
                    "yearly_stats": yearly_stats,
                    "year_range": {
                        "start": min_year + 1,
                        "end": max_year
                    }
                }
                
    except Exception as e:
        print(f"Error generating yearly stats: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))