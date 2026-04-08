"""
使用 PyFlink 连接 MySQL 进行数据分析 - 工作经验与薪资关系分析
如果 Flink 不可用，自动使用 MySQL 直接查询（备用方案）
"""
import pymysql
from flink_analysis import get_mysql_connection, get_flink_table_env, is_flink_available
from analysis_manager import check_table_exists_and_has_data, get_data_from_experience_salary_table, analyze_experience_salary, analyze_experience_salary_mysql, ensure_table_exists

# 全局标志：Flink 是否可用
_flink_available = None

def analysis_salary_distribution_stats(conn, filters):
    """
    计算薪资分布深度统计信息
    包括分位数、极值差、标准差等统计指标
    """
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # 获取所有经验段
    cursor.execute(f"""
        SELECT DISTINCT workingexp
        FROM jobs_51job
        WHERE {filters}
        AND workingexp IS NOT NULL AND workingexp <> ''
        ORDER BY workingexp
    """)
    
    experiences = [row['workingexp'] for row in cursor.fetchall()]
    distribution_stats = {}
    
    for exp in experiences:
        exp_filter = f"workingexp = '{exp}'"
        full_filter = filters + (" AND " + exp_filter if filters else exp_filter)
        
        # 获取该经验段的所有薪资数据
        cursor.execute(f"""
            SELECT salary_avg
            FROM jobs_51job
            WHERE {full_filter}
            AND salary_avg IS NOT NULL AND salary_avg > 0
            ORDER BY salary_avg
        """)
        
        salaries = [float(row['salary_avg']) for row in cursor.fetchall()]
        
        if not salaries:
            continue
            
        # 计算分位数
        n = len(salaries)
        percentiles = {}
        
        # 25%分位数
        p25_idx = int(n * 0.25)
        percentiles['p25'] = round(salaries[p25_idx], 2) if p25_idx < n else round(salaries[-1], 2)
        
        # 50%分位数（中位数）
        p50_idx = int(n * 0.50)
        percentiles['p50'] = round(salaries[p50_idx], 2) if p50_idx < n else round(salaries[-1], 2)
        
        # 75%分位数
        p75_idx = int(n * 0.75)
        percentiles['p75'] = round(salaries[p75_idx], 2) if p75_idx < n else round(salaries[-1], 2)
        
        # 90%分位数
        p90_idx = int(n * 0.90)
        percentiles['p90'] = round(salaries[p90_idx], 2) if p90_idx < n else round(salaries[-1], 2)
        
        # 计算极值差
        max_salary = round(max(salaries), 2)
        min_salary = round(min(salaries), 2)
        salary_range = round(max_salary - min_salary, 2)
        
        # 计算标准差
        import statistics
        std_dev = round(statistics.stdev(salaries), 2) if len(salaries) > 1 else 0
        
        # 计算变异系数（标准差/均值）
        mean_salary = round(statistics.mean(salaries), 2)
        cv = round((std_dev / mean_salary * 100), 2) if mean_salary > 0 else 0
        
        distribution_stats[exp] = {
            'percentiles': percentiles,
            'min_salary': min_salary,
            'max_salary': max_salary,
            'salary_range': salary_range,
            'mean_salary': mean_salary,
            'std_dev': std_dev,
            'coefficient_variation': cv,
            'sample_size': n
        }
    
    return distribution_stats


def analysis_exp_salary(city=None, job_type=None, edu=None):
    """
    分析4: 工作经验与薪资关系分析
    先检查ExperienceSalaryRelation表是否存在且有数据，如果有则从表中读取，否则使用Flink分析并存储

    参数:
    city: 城市筛选条件，默认为None（不筛选）
    job_type: 岗位类型筛选条件，默认为None（不筛选）
    edu: 学历要求筛选条件，默认为None（不筛选）

    返回:
    包含三个部分的字典:
    - chart_data: 原有的经验-薪资关系数据
    - top_salary_data: 新增的TOP N薪资对比表数据
    - distribution_stats: 新增的薪资分布深度统计数据
    """
    # 如果有筛选条件，直接执行分析（不使用缓存表）
    if city or job_type or edu:
        # 有筛选条件时，直接执行分析
        pass
    else:
        # 没有筛选条件时，确保表存在并检查是否有数据
        ensure_table_exists('ExperienceSalaryRelation')
        exists, has_data = check_table_exists_and_has_data('ExperienceSalaryRelation')
        
        if exists and has_data:
            # 从表中读取数据
            print("从ExperienceSalaryRelation表中读取数据")
            return get_data_from_experience_salary_table()
        
        # 如果表没有数据，执行分析并存储
        print("ExperienceSalaryRelation表没有数据，执行Flink分析...")
    
    # 构建筛选条件（Flink版本使用 <> 替代 !=）
    flink_filters = []
    flink_filters.append("workingexp IS NOT NULL AND TRIM(workingexp) <> ''")
    flink_filters.append("salary_avg IS NOT NULL AND salary_avg > 0")

    mysql_filters = []
    mysql_filters.append("workingexp IS NOT NULL AND workingexp <> ''")
    mysql_filters.append("salary_avg IS NOT NULL AND salary_avg > 0")

    if city:
        flink_filters.append(f"city = '{city}'")
        mysql_filters.append(f"city = '{city}'")

    if job_type:
        flink_filters.append(f"(job_title LIKE '%{job_type}%' OR job_desc LIKE '%{job_type}%')")
        mysql_filters.append(f"(job_title LIKE '%{job_type}%' OR job_desc LIKE '%{job_type}%')")

    if edu:
        flink_filters.append(f"edu = '{edu}'")
        mysql_filters.append(f"edu = '{edu}'")

    flink_filter_str = " AND ".join(flink_filters)
    mysql_filter_str = " AND ".join(mysql_filters)

    # 原有的分析数据（经验-平均薪资关系）
    chart_data = []
    # 新增的TOP N薪资对比表数据
    top_salary_data = {}
    # 新增的薪资分布深度统计数据
    distribution_stats = {}

    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 获取原有的经验-平均薪资数据
            result_table = t_env.sql_query(f"""
                SELECT 
                    workingexp AS experience,
                    AVG(salary_avg) AS avg_salary,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE {flink_filter_str}
                GROUP BY workingexp
                ORDER BY avg_salary DESC
            """)

            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    chart_data.append({
                        'experience': row[0] if row[0] else '未知',  # row[0] 对应 experience
                        'avg_salary': round(float(row[1]), 2) if row[1] else 0,  # row[1] 对应 avg_salary
                        'job_count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            # 使用MySQL获取TOP N数据（Flink SQL对于复杂的窗口函数支持有限）
            conn = get_mysql_connection()
            cursor = conn.cursor()

            # 获取所有经验段
            experiences = []
            for item in chart_data:
                experiences.append(item['experience'])

            for exp in experiences:
                # 构建经验段的过滤条件
                exp_filter = f"workingexp = '{exp}'"
                full_filter = mysql_filter_str + (" AND " + exp_filter if mysql_filter_str else exp_filter)

                # 查询该经验段薪资最高的前10个职位
                cursor.execute(f"""
                    SELECT 
                        job_title, company, salary_avg
                    FROM jobs_51job
                    WHERE {full_filter}
                    ORDER BY salary_avg DESC
                    LIMIT 10
                """)

                top_jobs = []
                max_salary = 0
                min_salary = 0

                for row in cursor.fetchall():
                    salary = round(float(row['salary_avg']), 2) if row['salary_avg'] else 0
                    top_jobs.append({
                        'job_title': row['job_title'] if row['job_title'] else '未知',
                        'company_name': row['company'] if row['company'] else '未知',
                        'salary': salary
                    })

                    if not max_salary or salary > max_salary:
                        max_salary = salary
                    if not min_salary or salary < min_salary:
                        min_salary = salary

                # 计算薪资极值差
                salary_range = round(max_salary - min_salary, 2) if max_salary and min_salary else 0

                top_salary_data[exp] = {
                    'top_jobs': top_jobs,
                    'salary_range': salary_range,
                    'max_salary': max_salary,
                    'min_salary': min_salary
                }

            # 获取薪资分布深度统计数据
            distribution_stats = analysis_salary_distribution_stats(conn, mysql_filter_str)
            
            conn.close()
            
            # 如果没有筛选条件，存储到表中
            if not city and not job_type and not edu:
                try:
                    analyze_experience_salary(t_env)
                except Exception as e:
                    print(f"存储到ExperienceSalaryRelation表失败: {e}")

            return {
                'chart_data': chart_data,
                'top_salary_data': top_salary_data,
                'distribution_stats': distribution_stats
            }
        except Exception as e:
            print(f"Flink 分析4执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 查询原有的经验-平均薪资数据
        cursor.execute(f"""
            SELECT 
                workingexp,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE {mysql_filter_str}
            GROUP BY workingexp
            ORDER BY avg_salary DESC
        """)

        for row in cursor.fetchall():
            chart_data.append({
                'experience': row['workingexp'] if row['workingexp'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })

        # 获取所有经验段
        experiences = []
        for item in chart_data:
            experiences.append(item['experience'])

        for exp in experiences:
            # 构建经验段的过滤条件
            exp_filter = f"workingexp = '{exp}'"
            full_filter = mysql_filter_str + (" AND " + exp_filter if mysql_filter_str else exp_filter)

            # 查询该经验段薪资最高的前10个职位
            cursor.execute(f"""
                SELECT 
                    job_title, company, salary_avg
                FROM jobs_51job
                WHERE {full_filter}
                ORDER BY salary_avg DESC
                LIMIT 10
            """)

            top_jobs = []
            max_salary = 0
            min_salary = 0

            for row in cursor.fetchall():
                salary = round(float(row['salary_avg']), 2) if row['salary_avg'] else 0
                top_jobs.append({
                    'job_title': row['job_title'] if row['job_title'] else '未知',
                    'company_name': row['company'] if row['company'] else '未知',
                    'salary': salary
                })

                if not max_salary or salary > max_salary:
                    max_salary = salary
                if not min_salary or salary < min_salary:
                    min_salary = salary

            # 计算薪资极值差
            salary_range = round(max_salary - min_salary, 2) if max_salary and min_salary else 0

            top_salary_data[exp] = {
                'top_jobs': top_jobs,
                'salary_range': salary_range,
                'max_salary': max_salary,
                'min_salary': min_salary
            }

        # 获取薪资分布深度统计数据
        distribution_stats = analysis_salary_distribution_stats(conn, mysql_filter_str)
        
        conn.close()
        
        # 如果没有筛选条件，存储到表中
        if not city and not job_type and not edu:
            try:
                analyze_experience_salary_mysql()
            except Exception as e:
                print(f"存储到ExperienceSalaryRelation表失败: {e}")
        
        return {
            'chart_data': chart_data,
            'top_salary_data': top_salary_data,
            'distribution_stats': distribution_stats
        }
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return {
            'chart_data': [],
            'top_salary_data': {},
            'distribution_stats': {}
        }




def analysis_salary_compliance(target_salary, experience):
    """
    薪资达标率分析
    计算特定经验段内薪资达到目标的职位占比

    参数:
    target_salary: 目标薪资
    experience: 工作经验段

    返回:
    包含达标和未达标数据的字典
    """
    if not target_salary or not experience:
        return {
            'target_salary': 0,
            'experience': '',
            'compliant_count': 0,
            'non_compliant_count': 0,
            'total_count': 0,
            'compliance_rate': 0
        }

    try:
        target_salary = float(target_salary)
    except ValueError:
        return {
            'target_salary': 0,
            'experience': '',
            'compliant_count': 0,
            'non_compliant_count': 0,
            'total_count': 0,
            'compliance_rate': 0
        }

    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            result_table = t_env.sql_query(f"""
                SELECT 
                    SUM(CASE WHEN salary_avg >= {target_salary} THEN 1 ELSE 0 END) AS compliant_count,
                    SUM(CASE WHEN salary_avg < {target_salary} THEN 1 ELSE 0 END) AS non_compliant_count,
                    COUNT(*) AS total_count
                FROM jobs_51job_source
                WHERE workingexp = '{experience}'
                    AND salary_avg IS NOT NULL AND salary_avg > 0
            """)

            with result_table.execute().collect() as results_iter:
                row = next(results_iter, None)
                if row:
                    # 使用索引访问而不是属性访问
                    compliant_count = int(row[0]) if row[0] else 0  # row[0] 对应 compliant_count
                    non_compliant_count = int(row[1]) if row[1] else 0  # row[1] 对应 non_compliant_count
                    total_count = int(row[2]) if row[2] else 0  # row[2] 对应 total_count
                    compliance_rate = round(compliant_count / total_count * 100, 2) if total_count > 0 else 0

                    return {
                        'target_salary': target_salary,
                        'experience': experience,
                        'compliant_count': compliant_count,
                        'non_compliant_count': non_compliant_count,
                        'total_count': total_count,
                        'compliance_rate': compliance_rate
                    }
        except Exception as e:
            print(f"Flink 薪资达标率分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN salary_avg >= %s THEN 1 ELSE 0 END) as compliant_count,
                SUM(CASE WHEN salary_avg < %s THEN 1 ELSE 0 END) as non_compliant_count,
                COUNT(*) as total_count
            FROM jobs_51job
            WHERE workingexp = %s
                AND salary_avg IS NOT NULL AND salary_avg > 0
        """, (target_salary, target_salary, experience))
        
        row = cursor.fetchone()
        if row:
            compliant_count = int(row['compliant_count']) if row['compliant_count'] else 0
            non_compliant_count = int(row['non_compliant_count']) if row['non_compliant_count'] else 0
            total_count = int(row['total_count']) if row['total_count'] else 0
            compliance_rate = round(compliant_count / total_count * 100, 2) if total_count > 0 else 0
            
            return {
                'target_salary': target_salary,
                'experience': experience,
                'compliant_count': compliant_count,
                'non_compliant_count': non_compliant_count,
                'total_count': total_count,
                'compliance_rate': compliance_rate
            }
        
        conn.close()
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
    
    return {
        'target_salary': target_salary,
        'experience': experience,
        'compliant_count': 0,
        'non_compliant_count': 0,
        'total_count': 0,
        'compliance_rate': 0
    }


def analysis_popular_jobs_salary_curve():
    """
    热门职位的经验 - 薪资曲线分析
    筛选投递量 Top10 的职位类别，分别展示各职位类别的经验 - 薪资曲线
    优先使用 PyFlink，如果不可用则使用 MySQL 直接查询
    """
    popular_jobs_salary_curve = {
        'job_categories': [],
        'salary_data': {}
    }

    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 获取投递量 Top15 的职位类别（为排除“客服专员”、“UI设计师”和“技术支持工程师”后保持足够数量）
            top_jobs_table = t_env.sql_query("""
                SELECT 
                    job_title,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE job_title IS NOT NULL 
                    AND job_title <> '' 
                    AND job_title <> '客服专员'
                    AND job_title <> 'UI设计师'
                    AND job_title <> '技术支持工程师'
                GROUP BY job_title
                ORDER BY job_count DESC
                LIMIT 15
            """)

            # 收集 Top10 职位类别
            job_categories = []
            with top_jobs_table.execute().collect() as results_iter:
                for row in results_iter:
                    job_title = row[0] if row[0] else '未知'
                    job_categories.append(job_title)
            
            # 只取前10个
            job_categories = job_categories[:10]

            if not job_categories:
                return popular_jobs_salary_curve

            # 为每个职位类别获取经验-薪资数据
            for job_category in job_categories:
                # 使用Flink查询该职位类别的经验-薪资数据
                salary_table = t_env.sql_query(f"""
                    SELECT 
                        workingexp AS experience,
                        AVG(salary_avg) AS avg_salary,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE job_title = '{job_category}'
                        AND workingexp IS NOT NULL AND workingexp <> ''
                        AND salary_avg IS NOT NULL AND salary_avg > 0
                    GROUP BY workingexp
                    ORDER BY experience
                """)

                # 收集经验-薪资数据
                salary_data = []
                with salary_table.execute().collect() as results_iter:
                    for row in results_iter:
                        experience = row[0] if row[0] else '未知'
                        avg_salary = round(float(row[1]), 2) if row[1] else 0
                        job_count = int(row[2]) if row[2] else 0
                        
                        # 对产品经理职位过滤掉特定经验段（强化8-9年过滤）
                        if job_category == '产品经理':
                            experience_str = str(experience).strip()
                            # 8-9年相关的所有可能变体
                            if (experience_str in ['8-9年', '8年以上', '9年', '无经验', '不限', '不限经验', '应届毕业生'] or 
                                '8-9年' in experience_str or 
                                '8年以上' in experience_str or
                                '9年' in experience_str or
                                ('8' in experience_str and '9' in experience_str and '年' in experience_str)):
                                continue
                        
                        # 过滤掉所有职位类别的“无工作经验”数据点（更严格的匹配）
                        experience_str = str(experience).strip()
                        if (experience_str in ['无经验', '不限', '不限经验', '应届毕业生', '无工作经验', '无需经验', '经验不限'] or 
                            '无经验' in experience_str or 
                            '不限经验' in experience_str or
                            '应届毕业生' in experience_str):
                            continue
                        
                        salary_data.append({
                            'experience': experience,
                            'avg_salary': avg_salary,
                            'job_count': job_count
                        })

                # 按经验排序
                salary_data.sort(key=lambda x: x['experience'])
                popular_jobs_salary_curve['salary_data'][job_category] = salary_data

            popular_jobs_salary_curve['job_categories'] = job_categories
            return popular_jobs_salary_curve
        except Exception as e:
            print(f"Flink 热门职位薪资曲线分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 获取投递量 Top15 的职位类别（为排除“客服专员”、“UI设计师”和“技术支持工程师”后保持足够数量）
        cursor.execute("""
            SELECT 
                job_title,
                COUNT(*) AS job_count
            FROM jobs_51job
            WHERE job_title IS NOT NULL 
                AND job_title <> '' 
                AND job_title <> '客服专员'
                AND job_title <> 'UI设计师'
                AND job_title <> '技术支持工程师'
            GROUP BY job_title
            ORDER BY job_count DESC
            LIMIT 15
        """)

        # 收集 Top10 职位类别
        job_categories = []
        for row in cursor.fetchall():
            job_title = row['job_title'] if row['job_title'] else '未知'
            job_categories.append(job_title)
        
        # 只取前10个
        job_categories = job_categories[:10]

        if not job_categories:
            return popular_jobs_salary_curve

        # 为每个职位类别获取经验-薪资数据
        for job_category in job_categories:
            cursor.execute("""
                SELECT 
                    workingexp AS experience,
                    AVG(salary_avg) AS avg_salary,
                    COUNT(*) AS job_count
                FROM jobs_51job
                WHERE job_title = %s
                    AND workingexp IS NOT NULL AND workingexp <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY workingexp
                ORDER BY experience
            """, (job_category,))

            # 收集经验-薪资数据
            salary_data = []
            for row in cursor.fetchall():
                experience = row['experience'] if row['experience'] else '未知'
                avg_salary = round(float(row['avg_salary']), 2) if row['avg_salary'] else 0
                job_count = int(row['job_count']) if row['job_count'] else 0
                
                # 对产品经理职位过滤掉特定经验段
                if job_category == '产品经理':
                    if experience in ['8-9年', '8年以上', '9年', '无经验', '不限', '不限经验', '应届毕业生']:
                        continue
                
                # 过滤掉所有职位类别的“无工作经验”数据点（更严格的匹配）
                experience_str = str(experience).strip()
                if (experience_str in ['无经验', '不限', '不限经验', '应届毕业生', '无工作经验', '无需经验', '经验不限'] or 
                    '无经验' in experience_str or 
                    '不限经验' in experience_str or
                    '应届毕业生' in experience_str):
                    continue
                
                salary_data.append({
                    'experience': experience,
                    'avg_salary': avg_salary,
                    'job_count': job_count
                })

            # 按经验排序
            salary_data.sort(key=lambda x: x['experience'])
            popular_jobs_salary_curve['salary_data'][job_category] = salary_data

        popular_jobs_salary_curve['job_categories'] = job_categories
        conn.close()
        return popular_jobs_salary_curve
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return popular_jobs_salary_curve


def sort_experience_by_length(experience):
    """
    按照经验时间长短排序（从长到短）
    返回排序用的数值，年限越长数值越大
    """
    import re
    
    if not experience or experience == '未知':
        return -100  # 最小的值，排在最后
    
    exp_str = str(experience).strip()
    
    # 优先匹配"年以上"格式（最高优先级）
    more_than_match = re.search(r'(\d+)年以上', exp_str)
    if more_than_match:
        base_year = int(more_than_match.group(1))
        return base_year + 50  # 加上大基数，确保排在最前面
    
    # 匹配范围格式如"3-5年"、"1~3年"
    range_match = re.search(r'(\d+)[-~](\d+)年', exp_str)
    if range_match:
        start_year = int(range_match.group(1))
        end_year = int(range_match.group(2))
        return (start_year + end_year) / 2  # 取中值
    
    # 匹配单个数字如"5年"
    single_match = re.search(r'(\d+)年', exp_str)
    if single_match:
        return int(single_match.group(1))
    
    # 处理特殊字符串
    if any(keyword in exp_str for keyword in ['无', '应届', '不限', '不要求', '经验']):
        return -50  # 无经验或应届生，排在较后
    
    # 尝试提取任何数字作为备选
    any_number = re.search(r'(\d+)', exp_str)
    if any_number:
        return int(any_number.group(1))
    
    return -100  # 完全无法识别的，排在最后


def analysis_exp_top_cities():
    """
    各经验段薪资TOP3城市排行榜分析
    为每个工作经验段列出平均薪资最高的3个城市
    优先使用 PyFlink，如果不可用则使用 MySQL 直接查询
    """
    exp_top_cities = {}

    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 获取所有经验段
            experiences_table = t_env.sql_query("""
                SELECT DISTINCT workingexp
                FROM jobs_51job_source
                WHERE workingexp IS NOT NULL AND workingexp <> ''
                ORDER BY workingexp
            """)

            experiences = []
            with experiences_table.execute().collect() as results_iter:
                for row in results_iter:
                    experience = row[0] if row[0] else '未知'
                    experiences.append(experience)

            if not experiences:
                return exp_top_cities
            
            # 按照经验时间长短排序（从长到短）
            experiences.sort(key=sort_experience_by_length, reverse=True)

            # 为每个经验段获取薪资最高的前3个城市
            for exp in experiences:
                top_cities_table = t_env.sql_query(f"""
                    SELECT 
                        city,
                        AVG(salary_avg) AS avg_salary,
                        COUNT(*) AS job_count
                    FROM jobs_51job_source
                    WHERE workingexp = '{exp}'
                        AND city IS NOT NULL AND city <> ''
                        AND salary_avg IS NOT NULL AND salary_avg > 0
                    GROUP BY city
                    ORDER BY avg_salary DESC
                    LIMIT 3
                """)

                top_cities = []
                with top_cities_table.execute().collect() as results_iter:
                    for row in results_iter:
                        city = row[0] if row[0] else '未知'
                        avg_salary = round(float(row[1]), 2) if row[1] else 0
                        job_count = int(row[2]) if row[2] else 0
                        
                        top_cities.append({
                            'city': city,
                            'avg_salary': avg_salary,
                            'job_count': job_count
                        })

                exp_top_cities[exp] = top_cities

            return exp_top_cities
        except Exception as e:
            print(f"Flink 各经验段薪资TOP3城市分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 获取所有经验段
        cursor.execute("""
            SELECT DISTINCT workingexp 
            FROM jobs_51job
            WHERE workingexp IS NOT NULL AND workingexp <> ''
            ORDER BY workingexp
        """)

        experiences = [row['workingexp'] for row in cursor.fetchall()]
        
        # 按照经验时间长短排序（从长到短）
        experiences.sort(key=sort_experience_by_length, reverse=True)

        for exp in experiences:
            # 查询该经验段薪资最高的前3个城市
            cursor.execute("""
                SELECT 
                    city,
                    AVG(salary_avg) as avg_salary,
                    COUNT(*) as job_count
                FROM jobs_51job
                WHERE workingexp = %s
                    AND city IS NOT NULL AND city <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                ORDER BY avg_salary DESC
                LIMIT 3
            """, (exp,))

            top_cities = []
            for row in cursor.fetchall():
                top_cities.append({
                    'city': row['city'] if row['city'] else '未知',
                    'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                    'job_count': int(row['job_count']) if row['job_count'] else 0
                })

            exp_top_cities[exp] = top_cities

        conn.close()
        return exp_top_cities
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return {}