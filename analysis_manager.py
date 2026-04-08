"""
分析管理器模块
负责检查分析结果表是否存在，创建表，并执行Flink分析将结果存储到表中
"""
import pymysql
from config import MYSQL_CONFIG, FLINK_MYSQL_CONFIG
from flink_analysis import get_mysql_connection, get_flink_table_env, is_flink_available
# 注意：不再导入这些函数以避免循环导入
# 这些函数现在在各自的模块中直接调用analysis_manager中的存储函数


def check_table_exists_and_has_data(table_name):
    """
    检查表是否存在且有数据
    返回: (exists, has_data) 元组
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        # 检查表是否存在
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = '{MYSQL_CONFIG['database']}' 
            AND table_name = '{table_name}'
        """)
        result = cursor.fetchone()
        if result['count'] == 0:
            cursor.close()
            return (False, False)
        
        # 检查表中是否有数据
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        result = cursor.fetchone()
        has_data = result['count'] > 0
        cursor.close()
        return (True, has_data)
    except Exception as e:
        print(f"检查表{table_name}时出错: {e}")
        return (False, False)
    finally:
        conn.close()


def get_data_from_city_salary_table():
    """
    从CitySalaryDistribution表中读取数据
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT city, avg_salary, job_count
            FROM CitySalaryDistribution 
            ORDER BY avg_salary DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })
        cursor.close()
        return results
    except Exception as e:
        print(f"从CitySalaryDistribution表读取数据失败: {e}")
        return []
    finally:
        conn.close()


def get_data_from_company_type_table():
    """
    从CompanyTypeDistribution表中读取数据
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT company_type, job_count, proportion 
            FROM CompanyTypeDistribution 
            ORDER BY job_count DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'company_type': row['company_type'] if row['company_type'] else '未知',
                'count': int(row['job_count']) if row['job_count'] else 0,
                'proportion': round(float(row['proportion']), 2) if row['proportion'] else 0
            })
        cursor.close()
        return results
    except Exception as e:
        print(f"从CompanyTypeDistribution表读取数据失败: {e}")
        return []
    finally:
        conn.close()


def get_data_from_education_table():
    """
    从EducationRequirementAnalysis表中读取数据
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT education, job_count, proportion
            FROM EducationRequirementAnalysis 
            ORDER BY job_count DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'edu': row['education'] if row['education'] else '未知',
                'count': int(row['job_count']) if row['job_count'] else 0,
                'percent': round(float(row['proportion']), 2) if row['proportion'] else 0
            })
        cursor.close()
        return results
    except Exception as e:
        print(f"从EducationRequirementAnalysis表读取数据失败: {e}")
        return []
    finally:
        conn.close()


def get_data_from_experience_salary_table():
    """
    从ExperienceSalaryRelation表中读取数据
    统计字段从jobs_51job表实时计算，不依赖表中的字段
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT experience, avg_salary, job_count
            FROM ExperienceSalaryRelation 
            ORDER BY avg_salary DESC
        """)
        chart_data = []
        top_salary_data = {}
        distribution_stats = {}
        
        import statistics
        
        for row in cursor.fetchall():
            exp = row['experience'] if row['experience'] else '未知'
            chart_data.append({
                'experience': exp,
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })
            
            if exp and exp != '未知':
                # 获取该经验段的所有薪资数据（用于计算统计信息）
                cursor.execute("""
                    SELECT salary_avg FROM jobs_51job
                    WHERE workingexp = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                    ORDER BY salary_avg
                """, (exp,))
                
                salaries = [float(r['salary_avg']) for r in cursor.fetchall()]
                n = len(salaries)
                
                if n > 0:
                    # 构建top_salary_data（取前10个最高薪资职位）
                    cursor.execute("""
                        SELECT job_title, company, salary_avg
                        FROM jobs_51job
                        WHERE workingexp = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                        ORDER BY salary_avg DESC
                        LIMIT 10
                    """, (exp,))
                    
                    top_jobs = []
                    for job_row in cursor.fetchall():
                        salary = round(float(job_row['salary_avg']), 2) if job_row['salary_avg'] else 0
                        top_jobs.append({
                            'job_title': job_row['job_title'] if job_row['job_title'] else '未知',
                            'company_name': job_row['company'] if job_row['company'] else '未知',
                            'salary': salary
                        })
                    
                    max_sal = max(salaries)
                    min_sal = min(salaries)
                    top_salary_data[exp] = {
                        'top_jobs': top_jobs,
                        'salary_range': round(max_sal - min_sal, 2),
                        'max_salary': round(max_sal, 2),
                        'min_salary': round(min_sal, 2)
                    }
                    
                    # 构建distribution_stats（从jobs_51job表实时计算）
                    # 计算分位数
                    p25_idx = int(n * 0.25)
                    p50_idx = int(n * 0.50)
                    p75_idx = int(n * 0.75)
                    p90_idx = int(n * 0.90)
                    
                    p25 = round(salaries[p25_idx], 2) if p25_idx < n else round(salaries[-1], 2)
                    p50 = round(salaries[p50_idx], 2) if p50_idx < n else round(salaries[-1], 2)
                    p75 = round(salaries[p75_idx], 2) if p75_idx < n else round(salaries[-1], 2)
                    p90 = round(salaries[p90_idx], 2) if p90_idx < n else round(salaries[-1], 2)
                    
                    # 计算统计值
                    min_salary = round(min(salaries), 2)
                    max_salary = round(max(salaries), 2)
                    salary_range = round(max_salary - min_salary, 2)
                    mean_salary = round(statistics.mean(salaries), 2)
                    std_dev = round(statistics.stdev(salaries), 2) if n > 1 else 0
                    coefficient_variation = round((std_dev / mean_salary * 100), 2) if mean_salary > 0 else 0
                    
                    distribution_stats[exp] = {
                        'percentiles': {
                            'p25': p25,
                            'p50': p50,
                            'p75': p75,
                            'p90': p90
                        },
                        'min_salary': min_salary,
                        'max_salary': max_salary,
                        'salary_range': salary_range,
                        'mean_salary': mean_salary,
                        'std_dev': std_dev,
                        'coefficient_variation': coefficient_variation,
                        'sample_size': n
                    }
        
        cursor.close()
        return {
            'chart_data': chart_data,
            'top_salary_data': top_salary_data,
            'distribution_stats': distribution_stats
        }
    except Exception as e:
        print(f"从ExperienceSalaryRelation表读取数据失败: {e}")
        import traceback
        traceback.print_exc()
        return {
            'chart_data': [],
            'top_salary_data': {},
            'distribution_stats': {}
        }
    finally:
        conn.close()


def ensure_table_exists(table_name):
    """
    确保指定的分析表存在，如果不存在则创建（但不填充数据）
    参数:
        table_name: 表名，支持 'CitySalaryDistribution', 'CompanyTypeDistribution', 
                   'EducationRequirementAnalysis', 'ExperienceSalaryRelation'
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        
        if table_name == 'CitySalaryDistribution':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS CitySalaryDistribution (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    city VARCHAR(50) NOT NULL,
                    avg_salary DECIMAL(10, 2) NOT NULL,
                    job_count INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
                    
        elif table_name == 'CompanyTypeDistribution':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS CompanyTypeDistribution (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    company_type VARCHAR(50) NOT NULL,
                    job_count INT NOT NULL,
                    proportion DECIMAL(5, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
                
        elif table_name == 'EducationRequirementAnalysis':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS EducationRequirementAnalysis (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    education VARCHAR(20) NOT NULL,
                    job_count INT NOT NULL,
                    proportion DECIMAL(5, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
                    
        elif table_name == 'ExperienceSalaryRelation':
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ExperienceSalaryRelation (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    experience VARCHAR(50) NOT NULL,
                    avg_salary DECIMAL(10, 2) NOT NULL,
                    job_count INT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
        
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"确保表{table_name}存在时出错: {e}")
        return False
    finally:
        conn.close()


def check_and_create_tables():
    """
    检查四个分析表是否存在，如果不存在则创建（已废弃，保留用于兼容）
    现在改为使用 ensure_table_exists() 按需创建
    """
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor()
        
        # 创建CitySalaryDistribution表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CitySalaryDistribution (
                id INT AUTO_INCREMENT PRIMARY KEY,
                city VARCHAR(50) NOT NULL,
                avg_salary DECIMAL(10, 2) NOT NULL,
                job_count INT NOT NULL,
                min_salary DECIMAL(10, 2) DEFAULT NULL,
                max_salary DECIMAL(10, 2) DEFAULT NULL,
                q25 DECIMAL(10, 2) DEFAULT NULL,
                q50 DECIMAL(10, 2) DEFAULT NULL,
                q75 DECIMAL(10, 2) DEFAULT NULL,
                q90 DECIMAL(10, 2) DEFAULT NULL,
                median_salary DECIMAL(10, 2) DEFAULT NULL,
                q75_salary DECIMAL(10, 2) DEFAULT NULL,
                province VARCHAR(50) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 为已存在的表添加新字段（如果不存在）
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN min_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN max_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN q25 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN q50 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN q75 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN q90 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN median_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN q75_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE CitySalaryDistribution ADD COLUMN province VARCHAR(50) DEFAULT NULL")
        except:
            pass
        
        # 创建CompanyTypeDistribution表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS CompanyTypeDistribution (
                id INT AUTO_INCREMENT PRIMARY KEY,
                company_type VARCHAR(50) NOT NULL,
                job_count INT NOT NULL,
                proportion DECIMAL(5, 2) NOT NULL,
                avg_salary DECIMAL(10, 2) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 为已存在的表添加新字段（如果不存在）
        try:
            cursor.execute("ALTER TABLE CompanyTypeDistribution ADD COLUMN avg_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        
        # 创建EducationRequirementAnalysis表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS EducationRequirementAnalysis (
                id INT AUTO_INCREMENT PRIMARY KEY,
                education VARCHAR(20) NOT NULL,
                job_count INT NOT NULL,
                proportion DECIMAL(5, 2) NOT NULL,
                avg_salary DECIMAL(10, 2) DEFAULT NULL,
                max_salary DECIMAL(10, 2) DEFAULT NULL,
                min_salary DECIMAL(10, 2) DEFAULT NULL,
                median_salary DECIMAL(10, 2) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 为已存在的表添加新字段（如果不存在）
        try:
            cursor.execute("ALTER TABLE EducationRequirementAnalysis ADD COLUMN avg_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE EducationRequirementAnalysis ADD COLUMN max_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE EducationRequirementAnalysis ADD COLUMN min_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE EducationRequirementAnalysis ADD COLUMN median_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        
        # 创建ExperienceSalaryRelation表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ExperienceSalaryRelation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                experience VARCHAR(50) NOT NULL,
                avg_salary DECIMAL(10, 2) NOT NULL,
                job_count INT NOT NULL,
                min_salary DECIMAL(10, 2) DEFAULT NULL,
                max_salary DECIMAL(10, 2) DEFAULT NULL,
                salary_range DECIMAL(10, 2) DEFAULT NULL,
                p25 DECIMAL(10, 2) DEFAULT NULL,
                p50 DECIMAL(10, 2) DEFAULT NULL,
                p75 DECIMAL(10, 2) DEFAULT NULL,
                p90 DECIMAL(10, 2) DEFAULT NULL,
                mean_salary DECIMAL(10, 2) DEFAULT NULL,
                std_dev DECIMAL(10, 2) DEFAULT NULL,
                coefficient_variation DECIMAL(10, 2) DEFAULT NULL,
                sample_size INT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 为已存在的表添加新字段（如果不存在）
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN min_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN max_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN salary_range DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN p25 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN p50 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN p75 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN p90 DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN mean_salary DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN std_dev DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN coefficient_variation DECIMAL(10, 2) DEFAULT NULL")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE ExperienceSalaryRelation ADD COLUMN sample_size INT DEFAULT NULL")
        except:
            pass
        
        conn.commit()
        print("四个分析表检查/创建完成")
        
        # 检查表中是否有数据
        tables = ['CitySalaryDistribution', 'CompanyTypeDistribution', 'EducationRequirementAnalysis', 'ExperienceSalaryRelation']
        empty_tables = []
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            result = cursor.fetchone()
            if result['count'] == 0:
                empty_tables.append(table)
        
        cursor.close()
        return empty_tables
        
    except Exception as e:
        print(f"检查/创建表时出错: {e}")
        return []
    finally:
        conn.close()


def analyze_and_store_results():
    """
    检查分析表是否存在/有数据，如果需要则执行分析并存储结果
    """
    # 检查并创建表，获取需要填充数据的表列表
    empty_tables = check_and_create_tables()
    
    # 如果所有表都有数据，则不需要执行分析
    if not empty_tables:
        print("所有分析表都已有数据，不需要执行分析")
        return
    
    print(f"需要为以下表填充数据: {', '.join(empty_tables)}")
    
    # 执行Flink分析并存储结果
    if is_flink_available():
        print("使用Flink执行分析...")
        
        # 获取Flink表环境
        t_env = get_flink_table_env()
        
        # 为需要填充数据的表执行分析
        if 'CitySalaryDistribution' in empty_tables:
            analyze_city_salary(t_env)
        
        if 'CompanyTypeDistribution' in empty_tables:
            analyze_company_type_data(t_env)
        
        if 'EducationRequirementAnalysis' in empty_tables:
            analyze_education_requirement(t_env)
        
        if 'ExperienceSalaryRelation' in empty_tables:
            analyze_experience_salary(t_env)
    else:
        print("Flink不可用，使用MySQL直接分析...")
        
        # 使用MySQL直接分析
        if 'CitySalaryDistribution' in empty_tables:
            analyze_city_salary_mysql()
        
        if 'CompanyTypeDistribution' in empty_tables:
            analyze_company_type_mysql()
        
        if 'EducationRequirementAnalysis' in empty_tables:
            analyze_education_requirement_mysql()
        
        if 'ExperienceSalaryRelation' in empty_tables:
            analyze_experience_salary_mysql()


def analyze_city_salary(t_env):
    """
    使用Flink分析城市薪资分布并存储到CitySalaryDistribution表
    """
    try:
        # 创建结果表
        result_table_ddl = f'''
        CREATE TABLE CitySalaryDistribution_sink (
            city VARCHAR(50),
            avg_salary DECIMAL(10, 2),
            job_count INT,
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://{FLINK_MYSQL_CONFIG["hostname"]}:{FLINK_MYSQL_CONFIG["port"]}/{FLINK_MYSQL_CONFIG["database-name"]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true',
            'table-name' = 'CitySalaryDistribution',
            'username' = '{FLINK_MYSQL_CONFIG["username"]}',
            'password' = '{FLINK_MYSQL_CONFIG["password"]}',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s'
        )
        '''
        t_env.execute_sql(result_table_ddl)
        
        # 执行分析并写入结果表
        t_env.sql_query('''
            SELECT 
                city, 
                CAST(AVG(salary_avg) AS DECIMAL(10, 2)) as avg_salary, 
                CAST(COUNT(*) AS INT) as job_count,
                CURRENT_TIMESTAMP as created_at
            FROM jobs_51job_source
            WHERE city IS NOT NULL AND TRIM(city) <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY city
            HAVING COUNT(*) >= 10
        ''').execute_insert('CitySalaryDistribution_sink')
        
        print("城市薪资分布分析结果已存储到CitySalaryDistribution表")
        
    except Exception as e:
        print(f"城市薪资分布分析失败: {e}")
        # 如果Flink失败，尝试使用MySQL
        analyze_city_salary_mysql()


def analyze_company_type_data(t_env):
    """
    使用Flink分析公司类型分布并存储到CompanyTypeDistribution表
    """
    try:
        # 创建结果表
        result_table_ddl = f'''
        CREATE TABLE CompanyTypeDistribution_sink (
            company_type VARCHAR(50),
            job_count INT,
            proportion DECIMAL(5, 2),
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://{FLINK_MYSQL_CONFIG["hostname"]}:{FLINK_MYSQL_CONFIG["port"]}/{FLINK_MYSQL_CONFIG["database-name"]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true',
            'table-name' = 'CompanyTypeDistribution',
            'username' = '{FLINK_MYSQL_CONFIG["username"]}',
            'password' = '{FLINK_MYSQL_CONFIG["password"]}',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s'
        )
        '''
        t_env.execute_sql(result_table_ddl)
        
        # 执行分析并写入结果表
        t_env.sql_query('''
            WITH total_count AS (
                SELECT COUNT(*) as total FROM jobs_51job_source
                WHERE company_type IS NOT NULL AND TRIM(company_type) <> ''
            )
            SELECT 
                company_type, 
                CAST(COUNT(*) AS INT) as job_count,
                CAST(ROUND(COUNT(*) * 100.0 / total, 2) AS DECIMAL(5, 2)) as proportion,
                CURRENT_TIMESTAMP as created_at
            FROM jobs_51job_source, total_count
            WHERE company_type IS NOT NULL AND TRIM(company_type) <> ''
            GROUP BY company_type, total
            ORDER BY job_count DESC
        ''').execute_insert('CompanyTypeDistribution_sink')
        
        print("公司类型分布分析结果已存储到CompanyTypeDistribution表")
        
    except Exception as e:
        print(f"公司类型分布分析失败: {e}")
        # 如果Flink失败，尝试使用MySQL
        analyze_company_type_mysql()


def analyze_education_requirement(t_env):
    """
    使用Flink分析学历要求并存储到EducationRequirementAnalysis表
    """
    try:
        # 创建结果表
        result_table_ddl = f'''
        CREATE TABLE EducationRequirementAnalysis_sink (
            education VARCHAR(20),
            job_count INT,
            proportion DECIMAL(5, 2),
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://{FLINK_MYSQL_CONFIG["hostname"]}:{FLINK_MYSQL_CONFIG["port"]}/{FLINK_MYSQL_CONFIG["database-name"]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true',
            'table-name' = 'EducationRequirementAnalysis',
            'username' = '{FLINK_MYSQL_CONFIG["username"]}',
            'password' = '{FLINK_MYSQL_CONFIG["password"]}',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s'
        )
        '''
        t_env.execute_sql(result_table_ddl)
        
        # 执行分析并写入结果表
        t_env.sql_query('''
            WITH total_count AS (
                SELECT COUNT(*) as total FROM jobs_51job_source
                WHERE edu IS NOT NULL AND TRIM(edu) <> ''
                    AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
            )
            SELECT 
                edu as education, 
                CAST(COUNT(*) AS INT) as job_count,
                CAST(ROUND(COUNT(*) * 100.0 / total, 2) AS DECIMAL(5, 2)) as proportion,
                CURRENT_TIMESTAMP as created_at
            FROM jobs_51job_source, total_count
            WHERE edu IS NOT NULL AND TRIM(edu) <> ''
                AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
            GROUP BY edu, total
            ORDER BY job_count DESC
        ''').execute_insert('EducationRequirementAnalysis_sink')
        
        print("学历要求分析结果已存储到EducationRequirementAnalysis表")
        
    except Exception as e:
        print(f"学历要求分析失败: {e}")
        # 如果Flink失败，尝试使用MySQL
        analyze_education_requirement_mysql()


def analyze_experience_salary(t_env):
    """
    使用Flink分析经验薪资关系并存储到ExperienceSalaryRelation表
    """
    try:
        # 创建结果表
        result_table_ddl = f'''
        CREATE TABLE ExperienceSalaryRelation_sink (
            experience VARCHAR(50),
            avg_salary DECIMAL(10, 2),
            job_count INT,
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://{FLINK_MYSQL_CONFIG["hostname"]}:{FLINK_MYSQL_CONFIG["port"]}/{FLINK_MYSQL_CONFIG["database-name"]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true',
            'table-name' = 'ExperienceSalaryRelation',
            'username' = '{FLINK_MYSQL_CONFIG["username"]}',
            'password' = '{FLINK_MYSQL_CONFIG["password"]}',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s'
        )
        '''
        t_env.execute_sql(result_table_ddl)
        
        # 执行分析并写入结果表
        t_env.sql_query('''
            SELECT 
                workingexp as experience, 
                CAST(AVG(salary_avg) AS DECIMAL(10, 2)) as avg_salary, 
                CAST(COUNT(*) AS INT) as job_count,
                CURRENT_TIMESTAMP as created_at
            FROM jobs_51job_source
            WHERE workingexp IS NOT NULL AND TRIM(workingexp) <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY workingexp
            ORDER BY avg_salary DESC
        ''').execute_insert('ExperienceSalaryRelation_sink')
        
        print("经验薪资关系分析结果已存储到ExperienceSalaryRelation表")
        
    except Exception as e:
        print(f"经验薪资关系分析失败: {e}")
        # 如果Flink失败，尝试使用MySQL
        analyze_experience_salary_mysql()


# MySQL备用方案
def analyze_city_salary_mysql():
    """
    使用MySQL分析城市薪资分布并存储到CitySalaryDistribution表
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 直接查询分析结果
        cursor.execute("""
            SELECT 
                city,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE city IS NOT NULL AND city <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY city
            HAVING COUNT(*) >= 10
            ORDER BY avg_salary DESC
        """)
        
        data = cursor.fetchall()
        
        # 清空表并插入新数据
        cursor.execute("TRUNCATE TABLE CitySalaryDistribution")
        
        for row in data:
            cursor.execute("""
                INSERT INTO CitySalaryDistribution 
                (city, avg_salary, job_count) 
                VALUES (%s, %s, %s)
            """, (row['city'], round(float(row['avg_salary']), 2), int(row['job_count'])))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("城市薪资分布分析结果已存储到CitySalaryDistribution表")
        
    except Exception as e:
        print(f"MySQL分析城市薪资分布失败: {e}")


def analyze_company_type_mysql():
    """
    使用MySQL分析公司类型分布并存储到CompanyTypeDistribution表
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 直接查询分析结果
        cursor.execute("""
            SELECT 
                company_type,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE company_type IS NOT NULL AND company_type <> ''
            GROUP BY company_type
            ORDER BY job_count DESC
        """)
        
        data = cursor.fetchall()
        
        # 计算总职位数和比例
        total_count = sum(row['job_count'] for row in data)
        
        # 清空表并插入新数据
        cursor.execute("TRUNCATE TABLE CompanyTypeDistribution")
        
        for row in data:
            proportion = round((row['job_count'] / total_count) * 100, 2) if total_count > 0 else 0
            cursor.execute(
                "INSERT INTO CompanyTypeDistribution (company_type, job_count, proportion) VALUES (%s, %s, %s)",
                (row['company_type'], row['job_count'], proportion)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("公司类型分布分析结果已存储到CompanyTypeDistribution表")
        
    except Exception as e:
        print(f"MySQL分析公司类型分布失败: {e}")


def analyze_education_requirement_mysql():
    """
    使用MySQL分析学历要求并存储到EducationRequirementAnalysis表
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 直接查询分析结果
        cursor.execute("""
            SELECT 
                edu,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE edu IS NOT NULL AND edu <> ''
                AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
            GROUP BY edu
            ORDER BY job_count DESC
        """)
        
        data = cursor.fetchall()
        
        # 计算总职位数
        total_count = sum(row['job_count'] for row in data)
        
        # 清空表并插入新数据
        cursor.execute("TRUNCATE TABLE EducationRequirementAnalysis")
        
        for row in data:
            proportion = round((row['job_count'] / total_count) * 100, 2) if total_count > 0 else 0
            cursor.execute("""
                INSERT INTO EducationRequirementAnalysis 
                (education, job_count, proportion) 
                VALUES (%s, %s, %s)
            """, (row['edu'], row['job_count'], proportion))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("学历要求分析结果已存储到EducationRequirementAnalysis表")
        
    except Exception as e:
        print(f"MySQL分析学历要求失败: {e}")


def analyze_experience_salary_mysql():
    """
    使用MySQL分析经验薪资关系并存储到ExperienceSalaryRelation表
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 直接查询分析结果
        cursor.execute("""
            SELECT 
                workingexp as experience,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE workingexp IS NOT NULL AND workingexp <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY workingexp
            ORDER BY avg_salary DESC
        """)
        
        data = cursor.fetchall()
        
        # 清空表并插入新数据
        cursor.execute("TRUNCATE TABLE ExperienceSalaryRelation")
        
        for row in data:
            cursor.execute("""
                INSERT INTO ExperienceSalaryRelation 
                (experience, avg_salary, job_count) 
                VALUES (%s, %s, %s)
            """, (row['experience'], round(float(row['avg_salary']), 2), int(row['job_count'])))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("经验薪资关系分析结果已存储到ExperienceSalaryRelation表")
        
    except Exception as e:
        print(f"MySQL分析经验薪资关系失败: {e}")
        import traceback
        traceback.print_exc()