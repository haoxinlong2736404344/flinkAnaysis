"""
学历分析模块
包含原始的学历要求分析及新增的交叉分析功能
"""
import pymysql
from flink_analysis import is_flink_available, get_flink_table_env, get_mysql_connection
from analysis_manager import check_table_exists_and_has_data, get_data_from_education_table, analyze_education_requirement, analyze_education_requirement_mysql, ensure_table_exists

def analysis_edu_requirement():
    """
    分析3: 学历要求分析
    按需检查表是否存在，如果有数据则从表中读取，否则使用Flink分析并存储
    """
    # 确保表存在（如果不存在则创建，但不填充数据）
    ensure_table_exists('EducationRequirementAnalysis')
    
    # 检查表是否有数据
    exists, has_data = check_table_exists_and_has_data('EducationRequirementAnalysis')
    
    if exists and has_data:
        # 从表中读取数据
        print("从EducationRequirementAnalysis表中读取数据")
        return get_data_from_education_table()
    
    # 如果表没有数据，执行分析并存储
    print("EducationRequirementAnalysis表没有数据，执行Flink分析...")
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修复：1. 使用 <> 替代 !=；2. 别名使用非关键字
            result_table = t_env.sql_query("""
                SELECT 
                    edu AS education,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE edu IS NOT NULL 
                    AND TRIM(edu) <> ''
                    AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
                GROUP BY edu
                ORDER BY job_count DESC
            """)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 修复：使用索引访问而不是属性访问
                    results.append({
                        'edu': row[0] if row[0] else '未知',  # row[0] 对应 education
                        'count': int(row[1]) if row[1] else 0  # row[1] 对应 job_count
                    })
            
            # 存储到表中
            try:
                analyze_education_requirement(t_env)
            except Exception as e:
                print(f"存储到EducationRequirementAnalysis表失败: {e}")

            return results
        except Exception as e:
            print(f"Flink 分析3执行失败，切换到 MySQL: {str(e)[:100]}")
            # 打印详细错误信息
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT 
                edu,
                COUNT(*) as count
            FROM jobs_51job
            WHERE edu IS NOT NULL AND edu <> ''
                AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
            GROUP BY edu
            ORDER BY count DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'edu': row['edu'] if row['edu'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })
        conn.close()
        
        # 存储到表中
        try:
            analyze_education_requirement_mysql()
        except Exception as e:
            print(f"存储到EducationRequirementAnalysis表失败: {e}")
        
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_edu_salary():
    """
    学历 - 薪资关联分析
    分析不同学历对应的薪资水平（平均薪资、薪资中位数、薪资最大值/最小值）
    """
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 1. 先获取基础薪资统计（平均、最大、最小、数量）
            result_table = t_env.sql_query("""
                SELECT 
                    edu AS education,
                    AVG(salary_avg) AS avg_salary,
                    MAX(salary_avg) AS max_salary,
                    MIN(salary_avg) AS min_salary,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE edu IS NOT NULL AND TRIM(edu) <> ''
                    AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY edu
                ORDER BY avg_salary DESC
            """)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 基础字段赋值
                    edu = row[0] if row[0] else '未知'
                    avg_salary = round(float(row[1]), 2) if row[1] else 0
                    max_salary = round(float(row[2]), 2) if row[2] else 0
                    min_salary = round(float(row[3]), 2) if row[3] else 0
                    job_count = int(row[4]) if row[4] else 0

                    # 2. 补充计算中位数（Flink 分支通过MySQL查询中位数）
                    median_salary = 0
                    try:
                        conn = get_mysql_connection()
                        cursor = conn.cursor(pymysql.cursors.DictCursor)
                        cursor.execute("""
                            SELECT salary_avg FROM jobs_51job
                            WHERE edu = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                            ORDER BY salary_avg
                        """, (edu,))
                        salary_rows = cursor.fetchall()
                        salaries = [float(row['salary_avg']) for row in salary_rows if row['salary_avg']]

                        if salaries:
                            n = len(salaries)
                            if n % 2 == 0:
                                median = (salaries[n//2 - 1] + salaries[n//2]) / 2
                            else:
                                median = salaries[n//2]
                            median_salary = round(median, 2)
                        conn.close()
                    except Exception as e:
                        print(f"Flink分支计算{edu}中位数失败: {e}")
                        median_salary = 0

                    # 组装结果
                    results.append({
                        'edu': edu,
                        'avg_salary': avg_salary,
                        'max_salary': max_salary,
                        'min_salary': min_salary,
                        'median_salary': median_salary,  # 关键：补充中位数字段
                        'job_count': job_count
                    })

            # 3. 计算学历提升幅度
            if len(results) > 1:
                edu_order = ['初中及以下', '高中', '中技', '中专', '大专', '本科', '硕士', '博士']
                results.sort(key=lambda x: edu_order.index(x['edu']) if x['edu'] in edu_order else 999)

                for i in range(1, len(results)):
                    prev = results[i-1]
                    curr = results[i]
                    if prev['avg_salary'] > 0:
                        increase_rate = ((curr['avg_salary'] - prev['avg_salary']) / prev['avg_salary']) * 100
                        curr['increase_rate'] = round(increase_rate, 2)
                    else:
                        curr['increase_rate'] = 0

            return results
        except Exception as e:
            print(f"Flink 学历薪资分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT 
                edu,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE edu IS NOT NULL AND edu <> ''
                AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY edu
            ORDER BY avg_salary DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'edu': row['edu'] if row['edu'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })

        # 计算中位数、最大值、最小值
        # 复用同一个连接，避免重复创建/关闭
        for result in results:
            edu = result['edu']

            # 获取中位数
            cursor.execute("""
                SELECT salary_avg FROM jobs_51job
                WHERE edu = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                ORDER BY salary_avg
            """, (edu,))
            salary_rows = cursor.fetchall()
            salaries = [float(row['salary_avg']) for row in salary_rows if row['salary_avg']]

            if salaries:
                n = len(salaries)
                if n % 2 == 0:
                    median = (salaries[n//2 - 1] + salaries[n//2]) / 2
                else:
                    median = salaries[n//2]
                result['median_salary'] = round(median, 2)
                result['max_salary'] = round(max(salaries), 2)
                result['min_salary'] = round(min(salaries), 2)
            else:
                result['median_salary'] = 0  # 确保字段存在，即使值为0
                result['max_salary'] = 0
                result['min_salary'] = 0

        # 计算学历提升幅度
        if len(results) > 1:
            edu_order = ['初中及以下', '高中', '中技', '中专', '大专', '本科', '硕士', '博士']
            results.sort(key=lambda x: edu_order.index(x['edu']) if x['edu'] in edu_order else 999)

            for i in range(1, len(results)):
                prev = results[i-1]
                curr = results[i]
                if prev['avg_salary'] > 0:
                    increase_rate = ((curr['avg_salary'] - prev['avg_salary']) / prev['avg_salary']) * 100
                    curr['increase_rate'] = round(increase_rate, 2)
                else:
                    curr['increase_rate'] = 0

        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        import traceback
        traceback.print_exc()
        return []

# 其他函数（analysis_edu_exp_cross/analysis_edu_city_cross）保持不变
def analysis_edu_exp_cross():
    """
    学历 - 工作经验交叉分析
    分析不同学历对应的工作经验要求分布
    """
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            result_table = t_env.sql_query("""
                SELECT 
                    edu AS education,
                    workingexp AS experience,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE edu IS NOT NULL AND TRIM(edu) <> ''
                    AND workingexp IS NOT NULL AND TRIM(workingexp) <> ''
                    AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
                GROUP BY edu, workingexp
                ORDER BY education, job_count DESC
            """)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 修复：使用索引访问而不是属性访问
                    results.append({
                        'edu': row[0] if row[0] else '未知',  # row[0] 对应 education
                        'workingexp': row[1] if row[1] else '未知',  # row[1] 对应 experience
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            return results
        except Exception as e:
            print(f"Flink 学历工作经验交叉分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT 
                edu,
                workingexp,
                COUNT(*) as count
            FROM jobs_51job
            WHERE edu IS NOT NULL AND edu <> ''
                AND workingexp IS NOT NULL AND workingexp <> ''
                AND edu IN ('大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士')
            GROUP BY edu, workingexp
            ORDER BY edu, count DESC
        """)

        results = []
        for row in cursor.fetchall():
            results.append({
                'edu': row['edu'] if row['edu'] else '未知',
                'workingexp': row['workingexp'] if row['workingexp'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })

        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_edu_city_cross(city=None):
    """
    学历 - 城市交叉分析
    分析各城市的学历要求分布，支持指定城市筛选
    """
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            if city:
                # 使用参数化查询防止SQL注入
                query = f"""
                    SELECT 
                        city AS city_name,
                        edu AS education,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE edu IS NOT NULL AND TRIM(edu) <> '' 
                        AND city IS NOT NULL AND TRIM(city) <> ''
                        AND city = '{city}'
                    GROUP BY city, edu
                    ORDER BY city_name, job_count DESC
                """
            else:
                query = """
                    SELECT 
                        city AS city_name,
                        edu AS education,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE edu IS NOT NULL AND TRIM(edu) <> '' 
                        AND city IS NOT NULL AND TRIM(city) <> ''
                    GROUP BY city, edu
                    ORDER BY city_name, job_count DESC
                """

            result_table = t_env.sql_query(query)

            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 修复：使用索引访问而不是属性访问
                    results.append({
                        'city': row[0] if row[0] else '未知',  # row[0] 对应 city_name
                        'edu': row[1] if row[1] else '未知',  # row[1] 对应 education
                        'count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            return results
        except Exception as e:
            print(f"Flink 学历城市交叉分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        where_clause = "WHERE edu IS NOT NULL AND edu <> '' AND city IS NOT NULL AND city <> ''"
        params = []

        if city:
            where_clause += " AND city = %s"
            params.append(city)

        cursor.execute(f"""
            SELECT 
                city,
                edu,
                COUNT(*) as count
            FROM jobs_51job
            {where_clause}
            GROUP BY city, edu
            ORDER BY city, count DESC
        """, params)

        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'edu': row['edu'] if row['edu'] else '未知',
                'count': int(row['count']) if row['count'] else 0
            })

        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []