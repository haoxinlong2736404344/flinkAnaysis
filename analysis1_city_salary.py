"""
城市薪资分布分析模块
包含以下功能：
1. 各城市薪资分布分析（原功能）
2. 城市薪资分位数深度分析（25%/50%/75%/90%分位）
3. 省级区域薪资聚合分析
4. 城市薪资梯队划分与统计
5. 多目标城市个性化对比工具
6. 所有城市平均薪资与职位数量展示
"""
import pymysql
from flink_analysis import get_mysql_connection, get_flink_table_env, is_flink_available
from analysis_manager import check_table_exists_and_has_data, get_data_from_city_salary_table, analyze_city_salary, analyze_city_salary_mysql, ensure_table_exists


def is_province_name(name):
    """判断一个名称是否是省份/自治区/直辖市/特别行政区"""
    if not name:
        return False

    name = str(name).strip()

    # 省份名称列表
    provinces = [
        '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省',
        '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省',
        '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省',
        '青海省', '台湾省', '内蒙古自治区', '广西壮族自治区', '西藏自治区',
        '宁夏回族自治区', '新疆维吾尔自治区', '香港特别行政区', '澳门特别行政区'
    ]

    # 直接匹配省份名称
    if name in provinces:
        return True

    # 检查是否以特定后缀结尾
    province_suffixes = ['省', '自治区', '特别行政区']
    for suffix in province_suffixes:
        if name.endswith(suffix):
            return True

    return False


def filter_out_provinces(data_list, city_field='city'):
    """过滤掉省份数据，只保留城市数据"""
    if not data_list:
        return []

    filtered_data = []
    for item in data_list:
        if isinstance(item, dict):
            city_name = item.get(city_field, '')
        else:
            city_name = getattr(item, city_field, '') if hasattr(item, city_field) else ''

        # 如果不是省份，则保留
        if not is_province_name(city_name):
            filtered_data.append(item)

    return filtered_data


def analysis_salary_by_city():
    """
    功能1：各城市薪资分布分析
    按需检查表是否存在，如果有数据则从表中读取，否则使用Flink分析并存储
    """
    # 确保表存在（如果不存在则创建，但不填充数据）
    ensure_table_exists('CitySalaryDistribution')
    
    # 检查表是否有数据
    exists, has_data = check_table_exists_and_has_data('CitySalaryDistribution')
    
    if exists and has_data:
        # 从表中读取数据
        print("从CitySalaryDistribution表中读取数据")
        results = get_data_from_city_salary_table()
        # 过滤掉省份数据
        results = filter_out_provinces(results)
        # 只返回前30个
        return results[:30] if len(results) > 30 else results
    
    # 如果表没有数据，执行分析并存储
    print("CitySalaryDistribution表没有数据，执行Flink分析...")
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修改：添加岗位数限制，只分析岗位数>=10的城市
            result_table = t_env.sql_query("""
                SELECT 
                    city AS city_name,
                    AVG(salary_avg) AS avg_salary,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE city IS NOT NULL AND TRIM(city) <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
                ORDER BY avg_salary DESC
            """)

            # 收集结果
            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    results.append({
                        'city': row[0] if row[0] else '未知',
                        'avg_salary': round(float(row[1]), 2) if row[1] else 0,
                        'job_count': int(row[2]) if row[2] else 0
                    })

            # 过滤掉省份数据
            results = filter_out_provinces(results)

            # 按平均薪资重新排序
            results.sort(key=lambda x: x['avg_salary'], reverse=True)

            # 只返回前30个
            results = results[:30] if len(results) > 30 else results
            
            # 存储到表中
            try:
                analyze_city_salary(t_env)
            except Exception as e:
                print(f"存储到CitySalaryDistribution表失败: {e}")
            
            return results
        except Exception as e:
            print(f"Flink 分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询（备用方案）
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        # 修改：添加岗位数限制
        cursor.execute("""
            SELECT 
                city,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE city IS NOT NULL AND city <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY city
            HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
            ORDER BY avg_salary DESC
            LIMIT 50
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })
        conn.close()

        # 过滤掉省份数据
        results = filter_out_provinces(results)

        # 只返回前30个
        results = results[:30] if len(results) > 30 else results
        
        # 存储到表中
        try:
            analyze_city_salary_mysql()
        except Exception as e:
            print(f"存储到CitySalaryDistribution表失败: {e}")
        
        return results
    except Exception as e:
        print(f"MySQL 查询失败: {e}")
        return []

def analysis_city_salary_quantiles():
    """
    功能2：城市薪资分位数深度分析（25%/50%/75%/90%分位）
    使用 PyFlink 计算各城市薪资的分位数，或使用 MySQL 的兼容方案计算分位数
    """
    # 检查 Flink 是否可用
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修改：添加岗位数限制
            result_table = t_env.sql_query("""
                SELECT 
                    city AS city_name,
                    COUNT(*) AS job_count,
                    AVG(salary_avg) AS avg_salary,
                    MAX(salary_avg) AS max_salary,
                    MIN(salary_avg) AS min_salary
                FROM jobs_51job_source
                WHERE city IS NOT NULL AND TRIM(city) <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
                ORDER BY avg_salary DESC
            """)

            # 收集结果
            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    results.append({
                        'city': row[0] if row[0] else '未知',
                        'job_count': int(row[1]) if row[1] else 0,
                        'avg_salary': round(float(row[2]), 2) if row[2] else 0,
                        'max_salary': round(float(row[3]), 2) if row[3] else 0,
                        'min_salary': round(float(row[4]), 2) if row[4] else 0,
                        'q25': 0,
                        'q50': 0,
                        'q75': 0,
                        'q90': 0
                    })

            # 过滤掉省份数据
            results = filter_out_provinces(results)

            # 在Python中计算分位数
            for result in results:
                city_name = result['city']
                if city_name and city_name != '未知':
                    try:
                        conn = get_mysql_connection()
                        cursor = conn.cursor(pymysql.cursors.DictCursor)
                        cursor.execute("""
                            SELECT salary_avg FROM jobs_51job
                            WHERE city = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                            ORDER BY salary_avg
                        """, (city_name,))

                        salaries = [float(row['salary_avg']) for row in cursor.fetchall()]

                        if salaries and len(salaries) >= 10:  # 确保有足够的数据
                            n = len(salaries)

                            # 计算25%分位数
                            q25_index = int(0.25 * n)
                            if q25_index < n:
                                result['q25'] = round(salaries[q25_index], 2)

                            # 计算中位数
                            if n % 2 == 0:
                                median = (salaries[n // 2 - 1] + salaries[n // 2]) / 2
                            else:
                                median = salaries[n // 2]
                            result['q50'] = round(median, 2)

                            # 计算75%分位数
                            q75_index = int(0.75 * n)
                            if q75_index < n:
                                result['q75'] = round(salaries[q75_index], 2)

                            # 计算90%分位数
                            q90_index = int(0.90 * n)
                            if q90_index < n:
                                result['q90'] = round(salaries[q90_index], 2)
                            pass

                        conn.close()
                    except Exception as e:
                        print(f"计算{city_name}分位数失败: {e}")

            return results
        except Exception as e:
            print(f"Flink 分位数分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            WITH city_salaries AS (
                SELECT 
                    city,
                    salary_avg,
                    COUNT(*) OVER (PARTITION BY city) as total,
                    ROW_NUMBER() OVER (PARTITION BY city ORDER BY salary_avg) as rn
                FROM jobs_51job
                WHERE city IS NOT NULL AND city <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
            ),
            city_quantiles AS (
                SELECT 
                    city,
                    AVG(CASE WHEN rn = CEIL(total * 0.25) THEN salary_avg END) as q25,
                    AVG(CASE WHEN rn IN (FLOOR((total + 1)/2), CEIL((total + 1)/2)) THEN salary_avg END) as q50,
                    AVG(CASE WHEN rn = CEIL(total * 0.75) THEN salary_avg END) as q75,
                    AVG(CASE WHEN rn = CEIL(total * 0.90) THEN salary_avg END) as q90,
                    MAX(total) as job_count
                FROM city_salaries
                GROUP BY city
                HAVING MAX(total) >= 10  -- 新增：只包含岗位数>=10的城市
            )
            SELECT * FROM city_quantiles ORDER BY q50 DESC;
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'q25': round(float(row['q25']), 2) if row['q25'] else 0,
                'q50': round(float(row['q50']), 2) if row['q50'] else 0,
                'q75': round(float(row['q75']), 2) if row['q75'] else 0,
                'q90': round(float(row['q90']), 2) if row['q90'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })
        conn.close()

        # 过滤掉省份数据
        results = filter_out_provinces(results)

        return results
    except Exception as e:
        print(f"MySQL 分位数查询失败: {e}")
        return []


def analysis_city_salary_tiers():
    """
    功能4：城市薪资梯队划分与统计
    自定义薪资区间，统计每个梯队的城市数量、岗位总数和平均薪资
    """
    # 修改梯队定义 - 使用新的标准
    TIER_DEFINITIONS = {
        '超高薪': {'min': 10000, 'max': 999999},  # 10000以上
        '高薪': {'min': 8500, 'max': 10000},    # 8500-10000
        '中高薪': {'min': 7000, 'max': 8500},   # 7000-8500
        '中等': {'min': 5500, 'max': 7000},      # 5500-7000
        '低薪': {'min': 0, 'max': 5500}          # 5500以下
    }

    # 检查 Flink 是否可用
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修改：添加岗位数限制
            result_table = t_env.sql_query("""
                SELECT 
                    city,
                    AVG(salary_avg) as avg_salary_city,
                    COUNT(*) as job_count_city
                FROM jobs_51job_source
                WHERE city IS NOT NULL AND TRIM(city) <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
            """)

            # 收集结果
            city_data = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    city_data.append({
                        'city': row[0] if row[0] else '未知',
                        'avg_salary_city': float(row[1]) if row[1] else 0,
                        'job_count_city': int(row[2]) if row[2] else 0
                    })

            # 过滤掉省份数据
            city_data = filter_out_provinces(city_data)

            # 在Python中进行梯队分类 - 使用新标准
            tier_stats = {}
            for city_info in city_data:
                salary = city_info['avg_salary_city']
                # 修改：使用新的梯队标准
                if salary >= 10000:
                    tier = '超高薪'
                elif salary >= 8500:
                    tier = '高薪'
                elif salary >= 7000:
                    tier = '中高薪'
                elif salary >= 5500:
                    tier = '中等'
                else:
                    tier = '低薪'

                if tier not in tier_stats:
                    tier_stats[tier] = {
                        'city_count': 0,
                        'total_job_count': 0,
                        'total_salary_sum': 0
                    }

                tier_stats[tier]['city_count'] += 1
                tier_stats[tier]['total_job_count'] += city_info['job_count_city']
                tier_stats[tier]['total_salary_sum'] += salary * city_info['job_count_city']

            # 构建最终结果
            results = []
            for tier, stats in tier_stats.items():
                if stats['city_count'] > 0:
                    avg_salary = stats['total_salary_sum'] / stats['total_job_count'] if stats['total_job_count'] > 0 else 0
                    results.append({
                        'salary_tier': tier,
                        'city_count': stats['city_count'],
                        'total_job_count': stats['total_job_count'],
                        'avg_salary': round(avg_salary, 2),
                        'tier_definition': TIER_DEFINITIONS.get(tier, {'min': 0, 'max': 0})
                    })

            # 按梯队排序
            tier_order = {'超高薪': 1, '高薪': 2, '中高薪': 3, '中等': 4, '低薪': 5}
            results.sort(key=lambda x: tier_order.get(x['salary_tier'], 6))

            return results
        except Exception as e:
            print(f"Flink 薪资梯队分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询（备用方案）
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            WITH city_avg_salary AS (
                SELECT 
                    city,
                    AVG(salary_avg) as avg_salary_city,
                    COUNT(*) as job_count_city
                FROM jobs_51job
                WHERE city IS NOT NULL AND city <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
            )
            SELECT 
                CASE 
                    -- 修改：使用新的梯队标准
                    WHEN avg_salary_city >= 10000 THEN '超高薪'
                    WHEN avg_salary_city >= 8500 AND avg_salary_city < 10000 THEN '高薪'
                    WHEN avg_salary_city >= 7000 AND avg_salary_city < 8500 THEN '中高薪'
                    WHEN avg_salary_city >= 5500 AND avg_salary_city < 7000 THEN '中等'
                    ELSE '低薪'
                END as salary_tier,
                COUNT(*) as city_count,
                SUM(job_count_city) as total_job_count,
                AVG(avg_salary_city) as avg_salary
            FROM city_avg_salary
            GROUP BY salary_tier
            ORDER BY 
                CASE salary_tier
                    WHEN '超高薪' THEN 1
                    WHEN '高薪' THEN 2
                    WHEN '中高薪' THEN 3
                    WHEN '中等' THEN 4
                    ELSE 5
                END
        """)
        results = []
        for row in cursor.fetchall():
            tier = row['salary_tier'] if row['salary_tier'] else '未知'
            results.append({
                'salary_tier': tier,
                'city_count': int(row['city_count']) if row['city_count'] else 0,
                'total_job_count': int(row['total_job_count']) if row['total_job_count'] else 0,
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'tier_definition': TIER_DEFINITIONS.get(tier, {'min': 0, 'max': 0})
            })
        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 薪资梯队查询失败: {e}")
        return []

def analysis_province_salary_aggregate():
    """
    功能3：省级区域薪资聚合分析
    对城市字段进行省份提取，统计各省的平均薪资、中位数薪资、岗位总数和城市数量
    """
    # 检查 Flink 是否可用
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 简化查询：先获取城市和薪资数据，省份提取在Python中进行
            result_table = t_env.sql_query("""
                SELECT 
                    city,
                    salary_avg
                FROM jobs_51job_source
                WHERE city IS NOT NULL AND TRIM(city) <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
            """)

            # 收集结果
            city_salaries = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    city_salaries.append({
                        'city': row[0] if row[0] else '未知',
                        'salary': float(row[1]) if row[1] else 0
                    })

            # 在Python中提取省份并聚合
            def extract_city_name(full_city):
                """提取城市名，去掉区域部分"""
                if not full_city:
                    return None
                # 去掉区域部分，只保留城市名
                if '-' in str(full_city):
                    return str(full_city).split('-')[0]
                return str(full_city)

            def extract_province(city_name):
                """增强版省份提取函数"""
                if not city_name:
                    return '其他'

                city_name = str(city_name).strip()

                # 直辖市和特别行政区
                direct_cities = {
                    '北京': '北京',
                    '上海': '上海',
                    '天津': '天津',
                    '重庆': '重庆',
                    '香港': '香港',
                    '澳门': '澳门'
                }
                if city_name in direct_cities:
                    return direct_cities[city_name]

                # 省份映射字典，包含主要城市
                province_mapping = {
                    # 广东省
                    '广州': '广东', '深圳': '广东', '东莞': '广东', '佛山': '广东', '珠海': '广东',
                    '中山': '广东', '惠州': '广东', '江门': '广东', '汕头': '广东', '湛江': '广东',
                    '茂名': '广东', '肇庆': '广东', '揭阳': '广东', '清远': '广东', '梅州': '广东',
                    '潮州': '广东', '韶关': '广东', '河源': '广东', '阳江': '广东', '云浮': '广东',
                    '汕尾': '广东',

                    # 江苏省
                    '南京': '江苏', '苏州': '江苏', '无锡': '江苏', '常州': '江苏', '徐州': '江苏',
                    '南通': '江苏', '连云港': '江苏', '淮安': '江苏', '盐城': '江苏', '扬州': '江苏',
                    '镇江': '江苏', '泰州': '江苏', '宿迁': '江苏', '昆山': '江苏',

                    # 浙江省
                    '杭州': '浙江', '宁波': '浙江', '温州': '浙江', '绍兴': '浙江', '湖州': '浙江',
                    '嘉兴': '浙江', '金华': '浙江', '衢州': '浙江', '舟山': '浙江', '台州': '浙江',
                    '丽水': '浙江',

                    # 四川省
                    '成都': '四川', '绵阳': '四川', '德阳': '四川', '广元': '四川', '遂宁': '四川',
                    '内江': '四川', '乐山': '四川', '南充': '四川', '眉山': '四川', '宜宾': '四川',
                    '广安': '四川', '达州': '四川', '雅安': '四川', '巴中': '四川', '资阳': '四川',
                    '阿坝': '四川', '甘孜': '四川', '凉山': '四川',

                    # 湖北省
                    '武汉': '湖北', '黄石': '湖北', '十堰': '湖北', '宜昌': '湖北', '襄阳': '湖北',
                    '鄂州': '湖北', '荆门': '湖北', '孝感': '湖北', '荆州': '湖北', '黄冈': '湖北',
                    '咸宁': '湖北', '随州': '湖北', '恩施': '湖北',

                    # 陕西省
                    '西安': '陕西', '铜川': '陕西', '宝鸡': '陕西', '咸阳': '陕西', '渭南': '陕西',
                    '延安': '陕西', '汉中': '陕西', '榆林': '陕西', '安康': '陕西', '商洛': '陕西',

                    # 河南省
                    '郑州': '河南', '开封': '河南', '洛阳': '河南', '平顶山': '河南', '安阳': '河南',
                    '鹤壁': '河南', '新乡': '河南', '焦作': '河南', '濮阳': '河南', '许昌': '河南',
                    '漯河': '河南', '三门峡': '河南', '南阳': '河南', '商丘': '河南', '信阳': '河南',
                    '周口': '河南', '驻马店': '河南',

                    # 山东省
                    '济南': '山东', '青岛': '山东', '淄博': '山东', '枣庄': '山东', '东营': '山东',
                    '烟台': '山东', '潍坊': '山东', '济宁': '山东', '泰安': '山东', '威海': '山东',
                    '日照': '山东', '滨州': '山东', '德州': '山东', '聊城': '山东', '临沂': '山东',
                    '菏泽': '山东',

                    # 湖南省
                    '长沙': '湖南', '株洲': '湖南', '湘潭': '湖南', '衡阳': '湖南', '邵阳': '湖南',
                    '岳阳': '湖南', '常德': '湖南', '张家界': '湖南', '益阳': '湖南', '郴州': '湖南',
                    '永州': '湖南', '怀化': '湖南', '娄底': '湖南', '湘西': '湖南',

                    # 福建省
                    '福州': '福建', '厦门': '福建', '莆田': '福建', '三明': '福建', '泉州': '福建',
                    '漳州': '福建', '南平': '福建', '龙岩': '福建', '宁德': '福建',

                    # 辽宁省
                    '沈阳': '辽宁', '大连': '辽宁', '鞍山': '辽宁', '抚顺': '辽宁', '本溪': '辽宁',
                    '丹东': '辽宁', '锦州': '辽宁', '营口': '辽宁', '阜新': '辽宁', '辽阳': '辽宁',
                    '盘锦': '辽宁', '铁岭': '辽宁', '朝阳': '辽宁', '葫芦岛': '辽宁',

                    # 黑龙江省
                    '哈尔滨': '黑龙江', '齐齐哈尔': '黑龙江', '鸡西': '黑龙江', '鹤岗': '黑龙江',
                    '双鸭山': '黑龙江', '大庆': '黑龙江', '伊春': '黑龙江', '佳木斯': '黑龙江',
                    '七台河': '黑龙江', '牡丹江': '黑龙江', '黑河': '黑龙江', '绥化': '黑龙江',
                    '大兴安岭': '黑龙江',

                    # 云南省
                    '昆明': '云南', '曲靖': '云南', '玉溪': '云南', '保山': '云南', '昭通': '云南',
                    '丽江': '云南', '普洱': '云南', '临沧': '云南', '楚雄': '云南', '红河': '云南',
                    '文山': '云南', '西双版纳': '云南', '大理': '云南', '德宏': '云南', '怒江': '云南',
                    '迪庆': '云南',

                    # 江西省
                    '南昌': '江西', '景德镇': '江西', '萍乡': '江西', '九江': '江西', '新余': '江西',
                    '鹰潭': '江西', '赣州': '江西', '吉安': '江西', '宜春': '江西', '抚州': '江西',
                    '上饶': '江西',

                    # 安徽省
                    '合肥': '安徽', '芜湖': '安徽', '蚌埠': '安徽', '淮南': '安徽', '马鞍山': '安徽',
                    '淮北': '安徽', '铜陵': '安徽', '安庆': '安徽', '黄山': '安徽', '滁州': '安徽',
                    '阜阳': '安徽', '宿州': '安徽', '六安': '安徽', '亳州': '安徽', '池州': '安徽',
                    '宣城': '安徽',

                    # 广西壮族自治区
                    '南宁': '广西', '柳州': '广西', '桂林': '广西', '梧州': '广西', '北海': '广西',
                    '防城港': '广西', '钦州': '广西', '贵港': '广西', '玉林': '广西', '百色': '广西',
                    '贺州': '广西', '河池': '广西', '来宾': '广西', '崇左': '广西',

                    # 贵州省
                    '贵阳': '贵州', '六盘水': '贵州', '遵义': '贵州', '安顺': '贵州', '毕节': '贵州',
                    '铜仁': '贵州', '黔西南': '贵州', '黔东南': '贵州', '黔南': '贵州',

                    # 山西省
                    '太原': '山西', '大同': '山西', '阳泉': '山西', '长治': '山西', '晋城': '山西',
                    '朔州': '山西', '晋中': '山西', '运城': '山西', '忻州': '山西', '临汾': '山西',
                    '吕梁': '山西',

                    # 河北省
                    '石家庄': '河北', '唐山': '河北', '秦皇岛': '河北', '邯郸': '河北', '邢台': '河北',
                    '保定': '河北', '张家口': '河北', '承德': '河北', '沧州': '河北', '廊坊': '河北',
                    '衡水': '河北',

                    # 甘肃省
                    '兰州': '甘肃', '嘉峪关': '甘肃', '金昌': '甘肃', '白银': '甘肃', '天水': '甘肃',
                    '武威': '甘肃', '张掖': '甘肃', '平凉': '甘肃', '酒泉': '甘肃', '庆阳': '甘肃',
                    '定西': '甘肃', '陇南': '甘肃', '临夏': '甘肃', '甘南': '甘肃',

                    # 海南省
                    '海口': '海南', '三亚': '海南', '三沙': '海南', '儋州': '海南',

                    # 吉林省
                    '长春': '吉林', '吉林': '吉林', '四平': '吉林', '辽源': '吉林', '通化': '吉林',
                    '白山': '吉林', '松原': '吉林', '白城': '吉林', '延边': '吉林',

                    # 宁夏回族自治区
                    '银川': '宁夏', '石嘴山': '宁夏', '吴忠': '宁夏', '固原': '宁夏', '中卫': '宁夏',

                    # 青海省
                    '西宁': '青海', '海东': '青海', '海北': '青海', '黄南': '青海', '海南': '青海',
                    '果洛': '青海', '玉树': '青海', '海西': '青海',

                    # 内蒙古自治区
                    '呼和浩特': '内蒙古', '包头': '内蒙古', '乌海': '内蒙古', '赤峰': '内蒙古',
                    '通辽': '内蒙古', '鄂尔多斯': '内蒙古', '呼伦贝尔': '内蒙古', '巴彦淖尔': '内蒙古',
                    '乌兰察布': '内蒙古', '兴安': '内蒙古', '锡林郭勒': '内蒙古', '阿拉善': '内蒙古',

                    # 新疆维吾尔自治区
                    '乌鲁木齐': '新疆', '克拉玛依': '新疆', '吐鲁番': '新疆', '哈密': '新疆',
                    '昌吉': '新疆', '博尔塔拉': '新疆', '巴音郭楞': '新疆', '阿克苏': '新疆',
                    '克孜勒苏': '新疆', '喀什': '新疆', '和田': '新疆', '伊犁': '新疆',
                    '塔城': '新疆', '阿勒泰': '新疆', '石河子': '新疆', '阿拉尔': '新疆',
                    '图木舒克': '新疆', '五家渠': '新疆', '北屯': '新疆', '铁门关': '新疆',
                    '双河': '新疆', '可克达拉': '新疆', '昆玉': '新疆', '胡杨河': '新疆',

                    # 西藏自治区
                    '拉萨': '西藏', '日喀则': '西藏', '昌都': '西藏', '林芝': '西藏', '山南': '西藏',
                    '那曲': '西藏', '阿里': '西藏'
                }

                # 检查城市名是否在映射字典中
                if city_name in province_mapping:
                    return province_mapping[city_name]

                # 特殊处理：城市名包含省份名的情况
                province_keywords = {
                    '广东': ['广东', '粤'],
                    '江苏': ['江苏', '苏'],
                    '浙江': ['浙江', '浙'],
                    '四川': ['四川', '川', '蜀'],
                    '湖北': ['湖北', '鄂'],
                    '陕西': ['陕西', '陕', '秦'],
                    '河南': ['河南', '豫'],
                    '山东': ['山东', '鲁'],
                    '湖南': ['湖南', '湘'],
                    '福建': ['福建', '闽'],
                    '辽宁': ['辽宁', '辽'],
                    '黑龙江': ['黑龙江', '黑'],
                    '云南': ['云南', '滇', '云'],
                    '江西': ['江西', '赣'],
                    '安徽': ['安徽', '皖'],
                    '广西': ['广西', '桂'],
                    '贵州': ['贵州', '黔', '贵'],
                    '山西': ['山西', '晋'],
                    '河北': ['河北', '冀'],
                    '甘肃': ['甘肃', '甘', '陇'],
                    '海南': ['海南', '琼'],
                    '吉林': ['吉林', '吉'],
                    '宁夏': ['宁夏', '宁'],
                    '青海': ['青海', '青'],
                    '内蒙古': ['内蒙古', '蒙'],
                    '新疆': ['新疆', '新'],
                    '西藏': ['西藏', '藏']
                }

                for province, keywords in province_keywords.items():
                    if any(keyword in city_name for keyword in keywords):
                        return province

                # 都不匹配则返回'其他'
                return '其他'

            # 按省份聚合数据
            province_stats = {}
            for item in city_salaries:
                # 先提取城市名
                main_city = extract_city_name(item['city'])
                # 再提取省份
                province = extract_province(main_city)

                if province not in province_stats:
                    province_stats[province] = {
                        'salaries': [],
                        'cities': set(),
                        'job_count': 0
                    }

                province_stats[province]['salaries'].append(item['salary'])
                province_stats[province]['cities'].add(main_city)  # 存储主要城市名
                province_stats[province]['job_count'] += 1

            # 计算统计数据
            results = []
            for province, stats in province_stats.items():
                if stats['salaries']:
                    # 计算平均薪资
                    avg_salary = sum(stats['salaries']) / len(stats['salaries'])

                    # 计算中位数
                    sorted_salaries = sorted(stats['salaries'])
                    n = len(sorted_salaries)
                    if n % 2 == 0:
                        median_salary = (sorted_salaries[n // 2 - 1] + sorted_salaries[n // 2]) / 2
                    else:
                        median_salary = sorted_salaries[n // 2]

                    results.append({
                        'province': province,
                        'avg_salary': round(avg_salary, 2),
                        'median_salary': round(median_salary, 2),
                        'job_count': stats['job_count'],
                        'city_count': len(stats['cities'])
                    })

            # 按平均薪资排序
            results.sort(key=lambda x: x['avg_salary'], reverse=True)
            return results

        except Exception as e:
            print(f"Flink 省级聚合分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询（兼容方案）
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            WITH province_salaries AS (
                SELECT 
                    -- 提取主要城市名
                    CASE 
                        WHEN INSTR(city, '-') > 0 THEN SUBSTRING(city, 1, INSTR(city, '-') - 1)
                        ELSE city
                    END as main_city,
                    salary_avg,
                    city
                FROM jobs_51job
                WHERE city IS NOT NULL AND city <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
            ),
            province_mapping AS (
                SELECT 
                    main_city,
                    salary_avg,
                    city,
                    CASE 
                        -- 直辖市和特别行政区
                        WHEN main_city IN ('北京', '上海', '天津', '重庆') THEN main_city
                        WHEN main_city IN ('香港', '澳门') THEN main_city

                        -- 广东省
                        WHEN main_city IN ('广州', '深圳', '东莞', '佛山', '珠海', '中山', '惠州', '江门', 
                                          '汕头', '湛江', '茂名', '肇庆', '揭阳', '清远', '梅州', '潮州', 
                                          '韶关', '河源', '阳江', '云浮', '汕尾') THEN '广东'

                        -- 江苏省
                        WHEN main_city IN ('南京', '苏州', '无锡', '常州', '徐州', '南通', '连云港', '淮安', 
                                          '盐城', '扬州', '镇江', '泰州', '宿迁', '昆山') THEN '江苏'

                        -- 浙江省
                        WHEN main_city IN ('杭州', '宁波', '温州', '绍兴', '湖州', '嘉兴', '金华', '衢州', 
                                          '舟山', '台州', '丽水') THEN '浙江'

                        -- 四川省
                        WHEN main_city IN ('成都', '绵阳', '德阳', '广元', '遂宁', '内江', '乐山', '南充', 
                                          '眉山', '宜宾', '广安', '达州', '雅安', '巴中', '资阳', '阿坝', 
                                          '甘孜', '凉山') THEN '四川'

                        -- 湖北省
                        WHEN main_city IN ('武汉', '黄石', '十堰', '宜昌', '襄阳', '鄂州', '荆门', '孝感', 
                                          '荆州', '黄冈', '咸宁', '随州', '恩施') THEN '湖北'

                        -- 陕西省
                        WHEN main_city IN ('西安', '铜川', '宝鸡', '咸阳', '渭南', '延安', '汉中', '榆林', 
                                          '安康', '商洛') THEN '陕西'

                        -- 河南省
                        WHEN main_city IN ('郑州', '开封', '洛阳', '平顶山', '安阳', '鹤壁', '新乡', '焦作', 
                                          '濮阳', '许昌', '漯河', '三门峡', '南阳', '商丘', '信阳', '周口', 
                                          '驻马店') THEN '河南'

                        -- 山东省
                        WHEN main_city IN ('济南', '青岛', '淄博', '枣庄', '东营', '烟台', '潍坊', '济宁', 
                                          '泰安', '威海', '日照', '滨州', '德州', '聊城', '临沂', '菏泽') THEN '山东'

                        -- 湖南省
                        WHEN main_city IN ('长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', 
                                          '益阳', '郴州', '永州', '怀化', '娄底', '湘西') THEN '湖南'

                        -- 福建省
                        WHEN main_city IN ('福州', '厦门', '莆田', '三明', '泉州', '漳州', '南平', '龙岩', 
                                          '宁德') THEN '福建'

                        -- 辽宁省
                        WHEN main_city IN ('沈阳', '大连', '鞍山', '抚顺', '本溪', '丹东', '锦州', '营口', 
                                          '阜新', '辽阳', '盘锦', '铁岭', '朝阳', '葫芦岛') THEN '辽宁'

                        -- 黑龙江省
                        WHEN main_city IN ('哈尔滨', '齐齐哈尔', '鸡西', '鹤岗', '双鸭山', '大庆', '伊春', 
                                          '佳木斯', '七台河', '牡丹江', '黑河', '绥化', '大兴安岭') THEN '黑龙江'

                        -- 云南省
                        WHEN main_city IN ('昆明', '曲靖', '玉溪', '保山', '昭通', '丽江', '普洱', '临沧', 
                                          '楚雄', '红河', '文山', '西双版纳', '大理', '德宏', '怒江', '迪庆') THEN '云南'

                        -- 江西省
                        WHEN main_city IN ('南昌', '景德镇', '萍乡', '九江', '新余', '鹰潭', '赣州', '吉安', 
                                          '宜春', '抚州', '上饶') THEN '江西'

                        -- 安徽省
                        WHEN main_city IN ('合肥', '芜湖', '蚌埠', '淮南', '马鞍山', '淮北', '铜陵', '安庆', 
                                          '黄山', '滁州', '阜阳', '宿州', '六安', '亳州', '池州', '宣城') THEN '安徽'

                        -- 广西壮族自治区
                        WHEN main_city IN ('南宁', '柳州', '桂林', '梧州', '北海', '防城港', '钦州', '贵港', 
                                          '玉林', '百色', '贺州', '河池', '来宾', '崇左') THEN '广西'

                        -- 贵州省
                        WHEN main_city IN ('贵阳', '六盘水', '遵义', '安顺', '毕节', '铜仁', '黔西南', 
                                          '黔东南', '黔南') THEN '贵州'

                        -- 山西省
                        WHEN main_city IN ('太原', '大同', '阳泉', '长治', '晋城', '朔州', '晋中', '运城', 
                                          '忻州', '临汾', '吕梁') THEN '山西'

                        -- 河北省
                        WHEN main_city IN ('石家庄', '唐山', '秦皇岛', '邯郸', '邢台', '保定', '张家口', 
                                          '承德', '沧州', '廊坊', '衡水') THEN '河北'

                        -- 甘肃省
                        WHEN main_city IN ('兰州', '嘉峪关', '金昌', '白银', '天水', '武威', '张掖', '平凉', 
                                          '酒泉', '庆阳', '定西', '陇南', '临夏', '甘南') THEN '甘肃'

                        -- 海南省
                        WHEN main_city IN ('海口', '三亚', '三沙', '儋州') THEN '海南'

                        -- 吉林省
                        WHEN main_city IN ('长春', '吉林', '四平', '辽源', '通化', '白山', '松原', '白城', 
                                          '延边') THEN '吉林'

                        -- 宁夏回族自治区
                        WHEN main_city IN ('银川', '石嘴山', '吴忠', '固原', '中卫') THEN '宁夏'

                        -- 青海省
                        WHEN main_city IN ('西宁', '海东', '海北', '黄南', '海南', '果洛', '玉树', '海西') THEN '青海'

                        -- 内蒙古自治区
                        WHEN main_city IN ('呼和浩特', '包头', '乌海', '赤峰', '通辽', '鄂尔多斯', '呼伦贝尔', 
                                          '巴彦淖尔', '乌兰察布', '兴安', '锡林郭勒', '阿拉善') THEN '内蒙古'

                        -- 新疆维吾尔自治区
                        WHEN main_city IN ('乌鲁木齐', '克拉玛依', '吐鲁番', '哈密', '昌吉', '博尔塔拉', 
                                          '巴音郭楞', '阿克苏', '克孜勒苏', '喀什', '和田', '伊犁', '塔城', 
                                          '阿勒泰', '石河子', '阿拉尔', '图木舒克', '五家渠', '北屯', 
                                          '铁门关', '双河', '可克达拉', '昆玉', '胡杨河') THEN '新疆'

                        -- 西藏自治区
                        WHEN main_city IN ('拉萨', '日喀则', '昌都', '林芝', '山南', '那曲', '阿里') THEN '西藏'

                        -- 特殊处理：包含省份关键字的情况
                        WHEN main_city LIKE '%广东%' OR main_city LIKE '%粤%' THEN '广东'
                        WHEN main_city LIKE '%江苏%' OR main_city LIKE '%苏%' THEN '江苏'
                        WHEN main_city LIKE '%浙江%' OR main_city LIKE '%浙%' THEN '浙江'
                        WHEN main_city LIKE '%四川%' OR main_city LIKE '%川%' OR main_city LIKE '%蜀%' THEN '四川'
                        WHEN main_city LIKE '%湖北%' OR main_city LIKE '%鄂%' THEN '湖北'
                        WHEN main_city LIKE '%陕西%' OR main_city LIKE '%陕%' OR main_city LIKE '%秦%' THEN '陕西'
                        WHEN main_city LIKE '%河南%' OR main_city LIKE '%豫%' THEN '河南'
                        WHEN main_city LIKE '%山东%' OR main_city LIKE '%鲁%' THEN '山东'
                        WHEN main_city LIKE '%湖南%' OR main_city LIKE '%湘%' THEN '湖南'
                        WHEN main_city LIKE '%福建%' OR main_city LIKE '%闽%' THEN '福建'
                        WHEN main_city LIKE '%辽宁%' OR main_city LIKE '%辽%' THEN '辽宁'
                        WHEN main_city LIKE '%黑龙江%' OR main_city LIKE '%黑%' THEN '黑龙江'
                        WHEN main_city LIKE '%云南%' OR main_city LIKE '%滇%' OR main_city LIKE '%云%' THEN '云南'
                        WHEN main_city LIKE '%江西%' OR main_city LIKE '%赣%' THEN '江西'
                        WHEN main_city LIKE '%安徽%' OR main_city LIKE '%皖%' THEN '安徽'
                        WHEN main_city LIKE '%广西%' OR main_city LIKE '%桂%' THEN '广西'
                        WHEN main_city LIKE '%贵州%' OR main_city LIKE '%黔%' OR main_city LIKE '%贵%' THEN '贵州'
                        WHEN main_city LIKE '%山西%' OR main_city LIKE '%晋%' THEN '山西'
                        WHEN main_city LIKE '%河北%' OR main_city LIKE '%冀%' THEN '河北'
                        WHEN main_city LIKE '%甘肃%' OR main_city LIKE '%甘%' OR main_city LIKE '%陇%' THEN '甘肃'
                        WHEN main_city LIKE '%海南%' OR main_city LIKE '%琼%' THEN '海南'
                        WHEN main_city LIKE '%吉林%' OR main_city LIKE '%吉%' THEN '吉林'
                        WHEN main_city LIKE '%宁夏%' OR main_city LIKE '%宁%' THEN '宁夏'
                        WHEN main_city LIKE '%青海%' OR main_city LIKE '%青%' THEN '青海'
                        WHEN main_city LIKE '%内蒙古%' OR main_city LIKE '%蒙%' THEN '内蒙古'
                        WHEN main_city LIKE '%新疆%' OR main_city LIKE '%新%' THEN '新疆'
                        WHEN main_city LIKE '%西藏%' OR main_city LIKE '%藏%' THEN '西藏'

                        ELSE '其他'
                    END as province
                FROM province_salaries
            ),
            province_agg AS (
                SELECT 
                    province,
                    AVG(salary_avg) as avg_salary,
                    COUNT(*) as job_count,
                    COUNT(DISTINCT main_city) as city_count,
                    AVG(CASE WHEN rn IN (FLOOR((total + 1)/2), CEIL((total + 1)/2)) THEN salary_avg END) as median_salary
                FROM (
                    SELECT 
                        province,
                        salary_avg,
                        main_city,
                        ROW_NUMBER() OVER (PARTITION BY province ORDER BY salary_avg) as rn,
                        COUNT(*) OVER (PARTITION BY province) as total
                    FROM province_mapping
                ) t
                GROUP BY province
            )
            SELECT * FROM province_agg ORDER BY avg_salary DESC;
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'province': row['province'] if row['province'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'median_salary': round(float(row['median_salary']), 2) if row['median_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0,
                'city_count': int(row['city_count']) if row['city_count'] else 0
            })
        conn.close()
        return results
    except Exception as e:
        print(f"MySQL 省级聚合查询失败: {e}")
        return []

def analysis_city_comparison(selected_cities):
    """
    功能5：多目标城市个性化对比工具
    支持用户选择3-5个城市进行横向对比
    """
    if not selected_cities or len(selected_cities) < 3 or len(selected_cities) > 5:
        print("错误：请选择3-5个城市进行对比")
        return []

    # 过滤掉省份（如果用户不小心选择了省份）
    selected_cities = [city for city in selected_cities if not is_province_name(city)]

    if len(selected_cities) < 3:
        print("错误：请选择至少3个城市进行对比（不能选择省份）")
        return []

    # 转义城市名中的单引号，避免SQL注入和语法错误
    escaped_cities = [city.replace("'", "''") for city in selected_cities]
    # 构建安全的IN条件
    city_conditions = ", ".join([f"'{city}'" for city in escaped_cities])

    # 检查 Flink 是否可用
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            # 修复：使用 <> 替代 !=，添加 TRIM 处理
            sql_query = f"""
                SELECT 
                    city AS city_name,
                    AVG(salary_avg) AS avg_salary,
                    COUNT(*) AS job_count,
                    MAX(salary_avg) AS max_salary,
                    MIN(salary_avg) AS min_salary,
                    MAX(salary_avg) - MIN(salary_avg) AS salary_range
                FROM jobs_51job_source
                WHERE city IN ({city_conditions})
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
            """
            result_table = t_env.sql_query(sql_query)

            # 收集结果
            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    # 使用索引访问而不是属性访问
                    results.append({
                        'city': row[0] if row[0] else '未知',
                        'avg_salary': round(float(row[1]), 2) if row[1] else 0,
                        'job_count': int(row[2]) if row[2] else 0,
                        'max_salary': round(float(row[3]), 2) if row[3] else 0,
                        'min_salary': round(float(row[4]), 2) if row[4] else 0,
                        'salary_range': round(float(row[5]), 2) if row[5] else 0,
                        'median_salary': 0,  # 在Python层面计算
                        'q75_salary': 0  # 在Python层面计算
                    })

            # 在Python层面计算中位数和75分位数
            for result in results:
                city_name = result['city']
                if city_name and city_name != '未知':
                    # 查询该城市的所有薪资
                    try:
                        conn = get_mysql_connection()
                        cursor = conn.cursor(pymysql.cursors.DictCursor)
                        cursor.execute("""
                            SELECT salary_avg FROM jobs_51job
                            WHERE city = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                            ORDER BY salary_avg
                        """, (city_name,))

                        salaries = [float(row['salary_avg']) for row in cursor.fetchall()]

                        if salaries:
                            # 计算中位数
                            n = len(salaries)
                            if n % 2 == 0:
                                median = (salaries[n // 2 - 1] + salaries[n // 2]) / 2
                            else:
                                median = salaries[n // 2]
                            result['median_salary'] = round(median, 2)

                            # 计算75分位数
                            q75_index = int(0.75 * n)
                            if q75_index < n:
                                result['q75_salary'] = round(salaries[q75_index], 2)
                            else:
                                result['q75_salary'] = round(salaries[-1], 2) if salaries else 0

                        conn.close()
                    except Exception as e:
                        print(f"计算{city_name}分位数失败: {e}")

            return results
        except Exception as e:
            print(f"Flink 多城市对比分析执行失败，切换到 MySQL: {str(e)[:100]}")
            import traceback
            traceback.print_exc()

    # 使用 MySQL 直接查询（改进方案）
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 方法1：先获取基本统计数据
        sql_query = f"""
            SELECT 
                city,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count,
                MAX(salary_avg) as max_salary,
                MIN(salary_avg) as min_salary,
                MAX(salary_avg) - MIN(salary_avg) as salary_range
            FROM jobs_51job
            WHERE city IN ({city_conditions})
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY city
        """
        cursor.execute(sql_query)
        results = []

        for row in cursor.fetchall():
            city_name = row['city']
            results.append({
                'city': city_name if city_name else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0,
                'max_salary': round(float(row['max_salary']), 2) if row['max_salary'] else 0,
                'min_salary': round(float(row['min_salary']), 2) if row['min_salary'] else 0,
                'salary_range': round(float(row['salary_range']), 2) if row['salary_range'] else 0,
                'median_salary': 0,
                'q75_salary': 0
            })

        # 方法2：为每个城市单独计算中位数和75分位数
        for result in results:
            city_name = result['city']
            if city_name and city_name != '未知':
                try:
                    # 查询该城市的所有薪资
                    cursor.execute("""
                        SELECT salary_avg FROM jobs_51job
                        WHERE city = %s AND salary_avg IS NOT NULL AND salary_avg > 0
                        ORDER BY salary_avg
                    """, (city_name,))

                    salary_rows = cursor.fetchall()
                    salaries = [float(row['salary_avg']) for row in salary_rows if row['salary_avg']]

                    if salaries:
                        n = len(salaries)

                        # 计算中位数
                        if n % 2 == 0:
                            median = (salaries[n // 2 - 1] + salaries[n // 2]) / 2
                        else:
                            median = salaries[n // 2]
                        result['median_salary'] = round(median, 2)

                        # 计算75分位数
                        q75_index = int(0.75 * n)
                        if q75_index < n:
                            result['q75_salary'] = round(salaries[q75_index], 2)
                        else:
                            result['q75_salary'] = round(salaries[-1], 2) if salaries else 0
                    else:
                        result['median_salary'] = 0
                        result['q75_salary'] = 0

                except Exception as e:
                    print(f"计算{city_name}分位数失败: {e}")
                    result['median_salary'] = 0
                    result['q75_salary'] = 0

        conn.close()
        return results

    except Exception as e:
        print(f"MySQL 多城市对比查询失败: {e}")
        return []


def get_all_cities():
    """
    获取所有城市列表，用于多城市对比工具的选择框
    """
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT DISTINCT city 
            FROM jobs_51job 
            WHERE city IS NOT NULL AND city <> ''
            ORDER BY city
        """)
        cities = [row['city'] for row in cursor.fetchall()]
        conn.close()

        # 过滤掉省份，只返回城市
        cities = [city for city in cities if not is_province_name(city)]

        return cities
    except Exception as e:
        print(f"获取城市列表失败: {e}")
        return []


def analysis_all_city_salary():
    """
    功能6：所有城市平均薪资与职位数量展示
    展示所有城市的平均薪资和职位数量
    """
    # 检查 Flink 是否可用
    if is_flink_available():
        try:
            t_env = get_flink_table_env()

            result_table = t_env.sql_query("""
                SELECT 
                    city AS city_name,
                    AVG(salary_avg) AS avg_salary,
                    COUNT(*) AS job_count
                FROM jobs_51job_source
                WHERE city IS NOT NULL AND TRIM(city) <> ''
                    AND salary_avg IS NOT NULL AND salary_avg > 0
                GROUP BY city
                HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
                ORDER BY avg_salary DESC
            """)

            # 收集结果
            results = []
            with result_table.execute().collect() as results_iter:
                for row in results_iter:
                    results.append({
                        'city': row[0] if row[0] else '未知',  # row[0] 对应 city_name
                        'avg_salary': round(float(row[1]), 2) if row[1] else 0,  # row[1] 对应 avg_salary
                        'job_count': int(row[2]) if row[2] else 0  # row[2] 对应 job_count
                    })

            # 过滤掉省份数据
            results = filter_out_provinces(results)

            return results
        except Exception as e:
            print(f"Flink 全城市薪资分析执行失败，切换到 MySQL: {str(e)[:100]}")

    # 使用 MySQL 直接查询（备用方案），返回所有城市
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT 
                city,
                AVG(salary_avg) as avg_salary,
                COUNT(*) as job_count
            FROM jobs_51job
            WHERE city IS NOT NULL AND city <> ''
                AND salary_avg IS NOT NULL AND salary_avg > 0
            GROUP BY city
            HAVING COUNT(*) >= 10  -- 新增：只包含岗位数>=10的城市
            ORDER BY avg_salary DESC
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                'city': row['city'] if row['city'] else '未知',
                'avg_salary': round(float(row['avg_salary']), 2) if row['avg_salary'] else 0,
                'job_count': int(row['job_count']) if row['job_count'] else 0
            })
        conn.close()

        # 过滤掉省份数据
        results = filter_out_provinces(results)

        return results
    except Exception as e:
        print(f"MySQL 全城市薪资查询失败: {e}")
        return []