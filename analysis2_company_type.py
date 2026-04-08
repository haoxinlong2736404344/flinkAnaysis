"""
公司类型分析模块
包含公司类型分布分析及新增的交叉分析功能
"""
import pymysql
from flink_analysis import is_flink_available, get_flink_table_env, get_mysql_connection
from analysis_manager import check_table_exists_and_has_data, get_data_from_company_type_table, analyze_company_type_data, analyze_company_type_mysql, ensure_table_exists

def analysis_company_type():
    """
    模块2: 公司类型分布分析
    按需检查表是否存在，如果有数据则从表中读取，否则使用Flink分析并存储
    """
    # 确保表存在（如果不存在则创建，但不填充数据）
    ensure_table_exists('CompanyTypeDistribution')
    
    # 检查表是否有数据
    exists, has_data = check_table_exists_and_has_data('CompanyTypeDistribution')
    
    if exists and has_data:
        # 从表中读取数据
        print("从CompanyTypeDistribution表中读取数据")
        return get_data_from_company_type_table()
    
    # 如果表没有数据，执行分析并存储
    print("CompanyTypeDistribution表没有数据，执行Flink分析...")
    if is_flink_available():
        try:
            t_env = get_flink_table_env()
            
            # 修复：使用 <> 替代 !=，使用别名
            result_table = t_env.sql_query("""
                SELECT 
                    company_type AS comp_type,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE company_type IS NOT NULL AND TRIM(company_type) <> ''
                GROUP BY company_type
                ORDER BY job_count DESC
            """)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    results.append({
                        'company_type': row[0] if row[0] else '未知',  # row[0] 对应 comp_type
                        'count': int(row[1]) if row[1] else 0  # row[1] 对应 job_count
                    })
            
            # 存储到表中
            try:
                analyze_company_type_data(t_env)
            except Exception as e:
                print(f"存储到CompanyTypeDistribution表失败: {e}")

            return results
        except Exception as e:
            print(f"Flink 分析2执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT 
                company_type,
                COUNT(*) as count
            FROM jobs_51job
            WHERE company_type IS NOT NULL AND company_type <> ''
            GROUP BY company_type
            ORDER BY count DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'company_type': row['company_type'] if row['company_type'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })
        conn.close()
        
        # 存储到表中
        try:
            analyze_company_type_mysql()
        except Exception as e:
            print(f"存储到CompanyTypeDistribution表失败: {e}")
        
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_company_city_cross(city=None):
    """
    公司类型 - 城市分布交叉分析
    分析各城市中不同公司类型的职位分布，支持指定城市筛选
    """
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修复：使用 <> 替代 !=，使用别名
            if city:
                query = f"""
                    SELECT 
                        city AS city_name,
                        company_type AS comp_type,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE company_type IS NOT NULL AND TRIM(company_type) <> '' 
                        AND city IS NOT NULL AND TRIM(city) <> ''
                        AND city = '{city}'
                    GROUP BY city, company_type
                    ORDER BY city_name, job_count DESC
                """
            else:
                query = """
                    SELECT 
                        city AS city_name,
                        company_type AS comp_type,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE company_type IS NOT NULL AND TRIM(company_type) <> '' 
                        AND city IS NOT NULL AND TRIM(city) <> ''
                    GROUP BY city, company_type
                    ORDER BY city_name, job_count DESC
                """

            result_table = t_env.sql_query(query)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    results.append({
                        'city': row[0] if row[0] else '未知',  # row[0] 对应 city_name
                        'company_type': row[1] if row[1] else '未知',  # row[1] 对应 comp_type
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            return results
        except Exception as e:
            print(f"Flink 公司类型城市交叉分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        where_clause = "WHERE company_type IS NOT NULL AND company_type <> '' AND city IS NOT NULL AND city <> ''"
        params = []

        if city:
            where_clause += " AND city = %s"
            params.append(city)

        cursor.execute(f"""
            SELECT 
                city,
                company_type,
                COUNT(*) as count
            FROM jobs_51job
            {where_clause}
            GROUP BY city, company_type
            ORDER BY city, count DESC
        """, params)

        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'company_type': row['company_type'] if row['company_type'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })

        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_company_size_cross(company_type=None):
    """
    公司类型 - 规模交叉分析
    分析不同规模的同类型公司的职位分布
    """
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修复：使用 <> 替代 !=，使用别名
            if company_type:
                query = f"""
                    SELECT 
                        company_type AS comp_type,
                        company_size AS comp_size,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE company_type IS NOT NULL AND TRIM(company_type) <> '' 
                        AND company_size IS NOT NULL AND TRIM(company_size) <> ''
                        AND company_type = '{company_type}'
                    GROUP BY company_type, company_size
                    ORDER BY comp_type, job_count DESC
                """
            else:
                query = """
                    SELECT 
                        company_type AS comp_type,
                        company_size AS comp_size,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE company_type IS NOT NULL AND TRIM(company_type) <> '' 
                        AND company_size IS NOT NULL AND TRIM(company_size) <> ''
                    GROUP BY company_type, company_size
                    ORDER BY comp_type, job_count DESC
                """

            result_table = t_env.sql_query(query)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    results.append({
                        'company_type': row[0] if row[0] else '未知',  # row[0] 对应 comp_type
                        'company_size': row[1] if row[1] else '未知',  # row[1] 对应 comp_size
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            return results
        except Exception as e:
            print(f"Flink 公司类型规模交叉分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        where_clause = "WHERE company_type IS NOT NULL AND company_type <> '' AND company_size IS NOT NULL AND company_size <> ''"
        params = []

        if company_type:
            where_clause += " AND company_type = %s"
            params.append(company_type)

        cursor.execute(f"""
            SELECT 
                company_type,
                company_size,
                COUNT(*) as count
            FROM jobs_51job
            {where_clause}
            GROUP BY company_type, company_size
            ORDER BY company_type, count DESC
        """, params)

        results = []
        for row in cursor.fetchall():
            results.append({
                'company_type': row['company_type'] if row['company_type'] else '未知',
                'company_size': row['company_size'] if row['company_size'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })

        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_top_company_requirements():
    """
    头部公司类型的职位学历 / 经验要求分析
    针对职位数量 TOP5 的公司类型，分析其对学历、工作经验的要求分布
    """
    # 首先获取职位数量 TOP5 的公司类型
    top_company_types = []

    # 使用 MySQL 获取 TOP5 公司类型（更高效）
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT company_type
            FROM jobs_51job
            WHERE company_type IS NOT NULL AND company_type <> ''
            GROUP BY company_type
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        for row in cursor.fetchall():
            top_company_types.append(row['company_type'])
        conn.close()
    except Exception as e:
        print(f"获取 TOP5 公司类型失败: {e}")
        return [], []

    if not top_company_types:
        return [], []

    # 学历要求分析
    edu_results = []
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 构建 IN 子句
            company_types_in = "'" + "', '" + "', '".join(top_company_types) + "'"

            result_table = t_env.sql_query(f"""
                SELECT 
                    company_type AS comp_type,
                    edu AS education,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE company_type IS NOT NULL AND TRIM(company_type) <> ''
                    AND company_type IN ({company_types_in})
                    AND edu IS NOT NULL AND TRIM(edu) <> ''
                GROUP BY company_type, edu
                ORDER BY comp_type, job_count DESC
            """)

            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    edu_results.append({
                        'company_type': row[0] if row[0] else '未知',  # row[0] 对应 comp_type
                        'edu': row[1] if row[1] else '未知',  # row[1] 对应 education
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })
        except Exception as e:
            print(f"Flink 公司类型学历要求分析执行失败，切换到 MySQL: {str(e)[:100]}")

    if not edu_results:
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            for company_type in top_company_types:
                cursor.execute("""
                    SELECT 
                        edu,
                        COUNT(*) as count
                    FROM jobs_51job
                    WHERE company_type = %s
                        AND edu IS NOT NULL AND edu <> ''
                    GROUP BY edu
                    ORDER BY count DESC
                """, (company_type,))

                for row in cursor.fetchall():
                    edu_results.append({
                        'company_type': company_type,
                        'edu': row['edu'] if row['edu'] else '未知',
                        'count': int(row['count']) if row['count'] else 0
                    })
            conn.close()
        except Exception as e:
            print(f"MySQL 公司类型学历要求分析失败: {e}")

    # 工作经验要求分析
    exp_results = []
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 构建 IN 子句
            company_types_in = "'" + "', '" + "', '".join(top_company_types) + "'"

            result_table = t_env.sql_query(f"""
                SELECT 
                    company_type AS comp_type,
                    workingexp AS experience,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE company_type IS NOT NULL AND TRIM(company_type) <> ''
                    AND company_type IN ({company_types_in})
                    AND workingexp IS NOT NULL AND TRIM(workingexp) <> ''
                GROUP BY company_type, workingexp
                ORDER BY comp_type, job_count DESC
            """)

            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    exp_results.append({
                        'company_type': row[0] if row[0] else '未知',  # row[0] 对应 comp_type
                        'workingexp': row[1] if row[1] else '未知',  # row[1] 对应 experience
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })
        except Exception as e:
            print(f"Flink 公司类型工作经验要求分析执行失败，切换到 MySQL: {str(e)[:100]}")

    if not exp_results:
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            for company_type in top_company_types:
                cursor.execute("""
                    SELECT 
                        workingexp,
                        COUNT(*) as count
                    FROM jobs_51job
                    WHERE company_type = %s
                        AND workingexp IS NOT NULL AND workingexp <> ''
                    GROUP BY workingexp
                    ORDER BY count DESC
                """, (company_type,))
                
                for row in cursor.fetchall():
                    exp_results.append({
                        'company_type': company_type,
                        'workingexp': row['workingexp'] if row['workingexp'] else '未知',
                        'count': int(row['count']) if row['count'] else 0
                    })
            conn.close()
        except Exception as e:
            print(f"MySQL 公司类型工作经验要求分析失败: {e}")
    
    return edu_results, exp_results