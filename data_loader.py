# """
# 数据导入模块 - 从 51job.xlsx 导入数据到 MySQL
# """
# import pandas as pd
# import pymysql
# from config import MYSQL_CONFIG
# import re
#
#
# def get_db():
#     """获取数据库连接"""
#     return pymysql.connect(
#         host=MYSQL_CONFIG['host'],
#         port=MYSQL_CONFIG['port'],
#         user=MYSQL_CONFIG['user'],
#         password=MYSQL_CONFIG['password'],
#         database=MYSQL_CONFIG['database'],
#         charset=MYSQL_CONFIG['charset']
#     )
#
#
# def parse_salary(salary_str):
#     """
#     解析薪资字符串，返回平均薪资（单位：元/月）
#     例如: "1.5-2万/月" -> 17500
#     """
#     if pd.isna(salary_str) or not salary_str:
#         return None
#
#     salary_str = str(salary_str).strip()
#
#     try:
#         if '万' in salary_str:
#             # 提取数字
#             parts = re.findall(r'[\d.]+', salary_str)
#             if len(parts) >= 2:
#                 # 范围薪资，取平均值
#                 min_salary = float(parts[0]) * 10000
#                 max_salary = float(parts[1]) * 10000
#                 return (min_salary + max_salary) / 2
#             elif len(parts) == 1:
#                 # 单个数值
#                 return float(parts[0]) * 10000
#         elif '千' in salary_str:
#             # 千元单位
#             parts = re.findall(r'[\d.]+', salary_str)
#             if len(parts) >= 2:
#                 min_salary = float(parts[0]) * 1000
#                 max_salary = float(parts[1]) * 1000
#                 return (min_salary + max_salary) / 2
#             elif len(parts) == 1:
#                 return float(parts[0]) * 1000
#     except:
#         pass
#
#     return None
#
#
# def create_jobs_table():
#     """创建职位数据表"""
#     conn = get_db()
#     try:
#         cursor = conn.cursor()
#
#         # 创建职位表
#         cursor.execute('''
#             CREATE TABLE IF NOT EXISTS jobs_51job (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 job_title VARCHAR(200),
#                 company VARCHAR(200),
#                 city VARCHAR(50),
#                 salary VARCHAR(100),
#                 salary_avg DECIMAL(10, 2),
#                 company_type VARCHAR(100),
#                 company_size VARCHAR(100),
#                 edu VARCHAR(50),
#                 workingexp VARCHAR(50),
#                 job_desc TEXT,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 INDEX idx_city (city),
#                 INDEX idx_company_type (company_type),
#                 INDEX idx_edu (edu),
#                 INDEX idx_workingexp (workingexp)
#             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
#         ''')
#
#         conn.commit()
#         cursor.close()
#         conn.close()
#         print("职位表创建成功！")
#     except Exception as e:
#         print(f"创建表错误: {e}")
#         raise
#
#
# def load_excel_to_mysql(excel_path='../flinkAnaysis/data/51job.xlsx'):
#     """
#     从 Excel 文件加载数据到 MySQL
#     """
#     try:
#         print(f"正在读取 Excel 文件: {excel_path}")
#         df = pd.read_excel(excel_path)
#
#         print(f"Excel 文件列名: {df.columns.tolist()}")
#         print(f"数据行数: {len(df)}")
#
#         # 创建表
#         create_jobs_table()
#
#         # 获取数据库连接
#         conn = get_db()
#         cursor = conn.cursor()
#
#         # 清空旧数据（可选）
#         cursor.execute('TRUNCATE TABLE jobs_51job')
#
#         # 准备插入数据
#         insert_sql = '''
#             INSERT INTO jobs_51job
#             (job_title, company, city, salary, salary_avg, company_type,
#              company_size, edu, workingexp, job_desc)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         '''
#
#         # 映射列名（根据实际 Excel 列名调整）
#         # 常见的列名映射
#         column_mapping = {
#             'job_title': ['职位', 'job_title', 'title', '职位名称', '岗位'],
#             'company': ['公司', 'company', '公司名称', '企业'],
#             'city': ['城市', 'city', '工作地点', '地点'],
#             'salary': ['薪资', 'salary', '工资', '薪酬'],
#             'company_type': ['公司类型', 'company_type', '企业类型', '类型'],
#             'company_size': ['公司规模', 'company_size', '规模', '人数'],
#             'edu': ['学历', 'edu', 'education', '学历要求', '教育背景'],
#             'workingexp': ['工作经验', 'workingexp', 'experience', '工作年限', '经验'],
#             'job_desc': ['职位描述', 'job_desc', 'description', '描述', '详情']
#         }
#
#         # 找到对应的列
#         actual_columns = {}
#         for key, possible_names in column_mapping.items():
#             for name in possible_names:
#                 if name in df.columns:
#                     actual_columns[key] = name
#                     break
#
#         print(f"列映射结果: {actual_columns}")
#
#         # 插入数据
#         inserted_count = 0
#         for index, row in df.iterrows():
#             try:
#                 job_title = str(row.get(actual_columns.get('job_title', ''), '')) if actual_columns.get('job_title') else ''
#                 company = str(row.get(actual_columns.get('company', ''), '')) if actual_columns.get('company') else ''
#                 city = str(row.get(actual_columns.get('city', ''), '')) if actual_columns.get('city') else ''
#                 salary = str(row.get(actual_columns.get('salary', ''), '')) if actual_columns.get('salary') else ''
#                 salary_avg = parse_salary(salary)
#                 company_type = str(row.get(actual_columns.get('company_type', ''), '')) if actual_columns.get('company_type') else ''
#                 company_size = str(row.get(actual_columns.get('company_size', ''), '')) if actual_columns.get('company_size') else ''
#                 edu = str(row.get(actual_columns.get('edu', ''), '')) if actual_columns.get('edu') else ''
#                 workingexp = str(row.get(actual_columns.get('workingexp', ''), '')) if actual_columns.get('workingexp') else ''
#                 job_desc = str(row.get(actual_columns.get('job_desc', ''), '')) if actual_columns.get('job_desc') else ''
#
#                 # 限制字符串长度
#                 job_title = job_title[:200] if job_title else ''
#                 company = company[:200] if company else ''
#                 city = city[:50] if city else ''
#                 salary = salary[:100] if salary else ''
#                 company_type = company_type[:100] if company_type else ''
#                 company_size = company_size[:100] if company_size else ''
#                 edu = edu[:50] if edu else ''
#                 workingexp = workingexp[:50] if workingexp else ''
#
#                 cursor.execute(insert_sql, (
#                     job_title, company, city, salary, salary_avg,
#                     company_type, company_size, edu, workingexp, job_desc
#                 ))
#                 inserted_count += 1
#
#                 if (inserted_count % 100) == 0:
#                     print(f"已插入 {inserted_count} 条数据...")
#
#             except Exception as e:
#                 print(f"插入第 {index+1} 行数据时出错: {e}")
#                 continue
#
#         conn.commit()
#         cursor.close()
#         conn.close()
#
#         print(f"数据导入完成！共导入 {inserted_count} 条记录")
#         return inserted_count
#
#     except Exception as e:
#         print(f"导入数据错误: {e}")
#         import traceback
#         traceback.print_exc()
#         raise
#
#
# if __name__ == '__main__':
#     # 测试导入
#     load_excel_to_mysql()
#
#---------------------------------------------------
"""
数据导入模块 - 从 51job.xlsx 导入数据到 MySQL
"""
import pandas as pd
import pymysql
import re
import os
from datetime import datetime
from config import MYSQL_CONFIG


def get_db():
    """获取数据库连接"""
    return pymysql.connect(
        host=MYSQL_CONFIG['host'],
        port=MYSQL_CONFIG['port'],
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password'],
        database=MYSQL_CONFIG['database'],
        charset=MYSQL_CONFIG['charset']
    )


def parse_salary(salary_str):
    """
    解析薪资字符串，返回平均薪资（单位：元/月）
    例如: "1.5-2万/月" -> 17500
    """
    if pd.isna(salary_str) or not salary_str:
        return None

    salary_str = str(salary_str).strip()

    try:
        if '万' in salary_str:
            parts = re.findall(r'[\d.]+', salary_str)
            if len(parts) >= 2:
                min_salary = float(parts[0]) * 10000
                max_salary = float(parts[1]) * 10000
                return (min_salary + max_salary) / 2
            elif len(parts) == 1:
                return float(parts[0]) * 10000
        elif '千' in salary_str:
            parts = re.findall(r'[\d.]+', salary_str)
            if len(parts) >= 2:
                min_salary = float(parts[0]) * 1000
                max_salary = float(parts[1]) * 1000
                return (min_salary + max_salary) / 2
            elif len(parts) == 1:
                return float(parts[0]) * 1000
    except Exception as e:
        print(f"解析薪资失败: {salary_str} -> {e}")

    return None


def _safe_cell_value(row, col_name):
    """安全获取单元格文本，NaN/None 统一返回空字符串"""
    if not col_name:
        return ''
    value = row.get(col_name, '')
    if pd.isna(value):
        return ''
    text = str(value).strip()
    if text.lower() == 'nan':
        return ''
    return text


def _normalize_edu(edu):
    text = (edu or '').strip()
    if not text:
        return '其他'
    if text in {'本科', '大专', '硕士', '博士', '中专', '高中', '中技', '初中及以下'}:
        return text
    if '博士' in text:
        return '博士'
    if '硕士' in text or '研究生' in text:
        return '硕士'
    if '本科' in text:
        return '本科'
    if '大专' in text or '专科' in text:
        return '大专'
    if '中专' in text:
        return '中专'
    if '中技' in text:
        return '中技'
    if '高中' in text:
        return '高中'
    if '初中' in text:
        return '初中及以下'
    if text in {'不限', '无要求'}:
        return '其他'
    return '其他'


def _normalize_workingexp(exp):
    text = (exp or '').strip()
    if not text:
        return '不限'
    text = text.replace('经验要求', '').replace('工作经验', '')
    if '不限' in text or '无经验' in text or '应届' in text:
        return '不限'
    if '1年以下' in text:
        return '1年以下'
    if '1-2' in text or '1至2' in text:
        return '1-2年'
    if '1-3' in text or '1至3' in text:
        return '1-3年'
    if '3-5' in text or '3至5' in text:
        return '3-5年'
    if '5-10' in text or '5至10' in text:
        return '5-10年'
    if '10年以上' in text or '10+' in text:
        return '10年以上'
    return text[:50]


def _normalize_city(city):
    text = (city or '').strip()
    if not text:
        return ''
    # 简单标准化：去掉末尾“市”
    if text.endswith('市') and len(text) > 1:
        return text[:-1]
    return text


def create_jobs_table():
    """创建职位数据表"""
    conn = get_db()
    try:
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs_51job (
                id INT AUTO_INCREMENT PRIMARY KEY,
                job_title VARCHAR(200),
                company VARCHAR(200),
                city VARCHAR(50),
                salary VARCHAR(100),
                salary_avg DECIMAL(10, 2),
                company_type VARCHAR(100),
                company_size VARCHAR(100),
                edu VARCHAR(50),
                workingexp VARCHAR(50),
                job_desc TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_city (city),
                INDEX idx_company_type (company_type),
                INDEX idx_edu (edu),
                INDEX idx_workingexp (workingexp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ 职位表创建/检查成功！")
    except Exception as e:
        print(f"❌ 创建表错误: {e}")
        raise


def create_quality_tables():
    """创建数据质量快照表与问题明细表"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_snapshot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(64) NOT NULL,
                stage VARCHAR(20) NOT NULL,
                total_count INT NOT NULL,
                missing_rate DECIMAL(8, 4) NOT NULL,
                duplicate_rate DECIMAL(8, 4) NOT NULL,
                salary_parse_success_rate DECIMAL(8, 4) NOT NULL,
                edu_valid_rate DECIMAL(8, 4) NOT NULL,
                outlier_rate DECIMAL(8, 4) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_batch_stage (batch_id, stage)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_quality_issues (
                id INT AUTO_INCREMENT PRIMARY KEY,
                batch_id VARCHAR(64) NOT NULL,
                stage VARCHAR(20) NOT NULL,
                issue_type VARCHAR(50) NOT NULL,
                row_ref VARCHAR(50),
                field_name VARCHAR(50),
                raw_value TEXT,
                fix_value TEXT,
                status VARCHAR(20) DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_batch_type (batch_id, issue_type),
                INDEX idx_stage (stage)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ 数据质量表创建/检查成功！")
    except Exception as e:
        print(f"❌ 创建数据质量表失败: {e}")
        raise


def calculate_quality_metrics(df):
    """计算数据质量核心指标"""
    total_count = len(df)
    if total_count == 0:
        return {
            'total_count': 0,
            'missing_rate': 0.0,
            'duplicate_rate': 0.0,
            'salary_parse_success_rate': 0.0,
            'edu_valid_rate': 0.0,
            'outlier_rate': 0.0
        }

    required_fields = ['job_title', 'company', 'city', 'salary', 'edu', 'workingexp']
    missing_rows = df[required_fields].isna().any(axis=1) | (df[required_fields] == '').any(axis=1)
    missing_rate = round(float(missing_rows.mean()) * 100, 4)

    duplicate_rate = round(float(df.duplicated(subset=['job_title', 'company', 'city', 'salary'], keep='first').mean()) * 100, 4)

    salary_non_empty = df['salary'].notna() & (df['salary'] != '')
    salary_non_empty_count = int(salary_non_empty.sum())
    salary_parse_ok_count = int((df['salary_avg'].notna() & salary_non_empty).sum())
    salary_parse_success_rate = round(
        (salary_parse_ok_count / salary_non_empty_count * 100) if salary_non_empty_count else 0.0,
        4
    )

    valid_edu_values = {'大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士', '其他'}
    edu_non_empty = df['edu'].notna() & (df['edu'] != '')
    edu_non_empty_count = int(edu_non_empty.sum())
    edu_valid_count = int(df['edu'].isin(valid_edu_values).sum())
    edu_valid_rate = round((edu_valid_count / edu_non_empty_count * 100) if edu_non_empty_count else 0.0, 4)

    outlier_mask = df['salary_avg'].notna() & ((df['salary_avg'] < 1000) | (df['salary_avg'] > 100000))
    outlier_rate = round(float(outlier_mask.mean()) * 100, 4)

    return {
        'total_count': int(total_count),
        'missing_rate': missing_rate,
        'duplicate_rate': duplicate_rate,
        'salary_parse_success_rate': salary_parse_success_rate,
        'edu_valid_rate': edu_valid_rate,
        'outlier_rate': outlier_rate
    }


def save_quality_snapshot(cursor, batch_id, stage, metrics):
    """保存数据质量快照"""
    cursor.execute(
        '''
        INSERT INTO data_quality_snapshot
        (batch_id, stage, total_count, missing_rate, duplicate_rate,
         salary_parse_success_rate, edu_valid_rate, outlier_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        (
            batch_id, stage, metrics['total_count'], metrics['missing_rate'],
            metrics['duplicate_rate'], metrics['salary_parse_success_rate'],
            metrics['edu_valid_rate'], metrics['outlier_rate']
        )
    )


def save_issue_records(cursor, batch_id, stage, issues):
    """批量写入问题明细"""
    if not issues:
        return
    cursor.executemany(
        '''
        INSERT INTO data_quality_issues
        (batch_id, stage, issue_type, row_ref, field_name, raw_value, fix_value, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'open')
        ''',
        issues
    )


def load_excel_to_mysql(excel_path='../flinkAnaysis/data/51job.xlsx'):
    """
    从 Excel 文件加载数据到 MySQL
    """
    # 1. 验证文件路径（关键修复：路径错误直接提示）
    excel_path = os.path.abspath(excel_path)
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"❌ Excel文件不存在！路径：{excel_path}\n请检查文件路径是否正确")

    try:
        print(f"📖 正在读取 Excel 文件: {excel_path}")
        df = pd.read_excel(excel_path)

        # 2. 打印Excel原始列名（去空格/特殊字符，便于匹配）
        raw_columns = df.columns.tolist()
        clean_columns = [col.strip().replace(' ', '').replace('-', '').replace('_', '') for col in raw_columns]
        print(f"📋 Excel 原始列名: {raw_columns}")
        print(f"🧹 Excel 清洗后列名: {clean_columns}")
        print(f"📊 数据总行数: {len(df)}")

        # 创建业务表与质量表
        create_jobs_table()
        create_quality_tables()

        # 3. 优化列映射（支持清洗后列名匹配，增加更多常见别名）
        column_mapping = {
            'job_title': ['职位', 'job_title', 'title', '职位名称', '岗位', '岗位名称', 'job_name'],
            # 新增'job_name'匹配截图列名
            'company': ['公司', 'company', '公司名称', '企业', '企业名称', 'company_name'],  # 新增'company_name'匹配截图列名
            'job_desc': ['职位描述', 'job_desc', 'description', '描述', '详情', '岗位职责', 'require_content'],
            # 新增'require_content'匹配职位描述列
            # 其他字段保持不变
            'city': ['城市', 'city', '工作地点', '地点'],
            'salary': ['薪资', 'salary', '工资', '薪酬'],
            'salary_avg_raw': ['salary_avg', '平均薪资', 'salaryavg'],
            'company_type': ['公司类型', 'company_type', '企业类型', '类型'],
            'company_size': ['公司规模', 'company_size', '规模', '人数'],
            'edu': ['学历', 'edu', 'education', '学历要求', '教育背景'],
            'workingexp': ['工作经验', 'workingexp', 'experience', '工作年限', '经验']
        }

        # 4. 匹配列名（支持清洗后匹配，记录未匹配字段）
        actual_columns = {}
        unmatched_fields = []
        for db_field, possible_names in column_mapping.items():
            # 清洗可能的列名，和Excel清洗后的列名匹配
            matched = False
            for name in possible_names:
                clean_name = name.strip().replace(' ', '').replace('-', '').replace('_', '')
                if clean_name in clean_columns:
                    # 找到原始列名（不是清洗后的）
                    raw_idx = clean_columns.index(clean_name)
                    actual_columns[db_field] = raw_columns[raw_idx]
                    matched = True
                    break
            if not matched:
                unmatched_fields.append(db_field)

        # 打印列映射结果（关键：提示未匹配字段）
        print(f"🔍 列映射结果: {actual_columns}")
        if unmatched_fields:
            print(f"⚠️  以下字段未匹配到Excel列（将填充空值）: {unmatched_fields}")

        # 4. 统一整理成规范字段 DataFrame（便于质量评估与治理）
        standard_rows = []
        for index, row in df.iterrows():
            job_title = _safe_cell_value(row, actual_columns.get('job_title'))
            company = _safe_cell_value(row, actual_columns.get('company'))
            city = _safe_cell_value(row, actual_columns.get('city'))
            salary = _safe_cell_value(row, actual_columns.get('salary'))
            salary_avg_raw = _safe_cell_value(row, actual_columns.get('salary_avg_raw'))
            company_type = _safe_cell_value(row, actual_columns.get('company_type'))
            company_size = _safe_cell_value(row, actual_columns.get('company_size'))
            edu = _safe_cell_value(row, actual_columns.get('edu'))
            workingexp = _safe_cell_value(row, actual_columns.get('workingexp'))
            job_desc = _safe_cell_value(row, actual_columns.get('job_desc'))

            standard_rows.append({
                'row_ref': str(index + 2),  # +2 对齐 Excel 可视行号（含表头）
                'job_title': job_title,
                'company': company,
                'city': city,
                'salary': salary,
                'salary_avg_raw': salary_avg_raw,
                'company_type': company_type,
                'company_size': company_size,
                'edu': edu,
                'workingexp': workingexp,
                'job_desc': job_desc
            })
        standard_df = pd.DataFrame(standard_rows)
        # before：只按 salary 字段解析，作为“治理前”基线
        standard_df['salary_avg'] = standard_df['salary'].apply(parse_salary)

        # 5. 生成治理前指标与问题明细
        batch_id = datetime.now().strftime('%Y%m%d%H%M%S')
        before_metrics = calculate_quality_metrics(standard_df)
        print(f"🧪 质量快照(before): {before_metrics}")

        issues = []
        required_fields = ['job_title', 'company', 'city', 'salary', 'edu', 'workingexp']
        missing_mask = standard_df[required_fields].isna().any(axis=1) | (standard_df[required_fields] == '').any(axis=1)
        for _, item in standard_df[missing_mask].head(300).iterrows():
            issues.append((batch_id, 'before', 'missing_required_field', item['row_ref'], '', '', '',))

        duplicate_mask = standard_df.duplicated(subset=['job_title', 'company', 'city', 'salary'], keep='first')
        for _, item in standard_df[duplicate_mask].head(300).iterrows():
            issues.append((batch_id, 'before', 'duplicate_record', item['row_ref'], '', '', '',))

        salary_parse_fail_mask = (standard_df['salary'] != '') & standard_df['salary_avg'].isna()
        for _, item in standard_df[salary_parse_fail_mask].head(300).iterrows():
            issues.append((batch_id, 'before', 'salary_parse_failed', item['row_ref'], 'salary', item['salary'][:1000], '',))

        valid_edu_values = {'大专', '本科', '高中', '中专', '初中及以下', '中技', '硕士', '博士', '其他'}
        invalid_edu_mask = (standard_df['edu'] != '') & (~standard_df['edu'].isin(valid_edu_values))
        for _, item in standard_df[invalid_edu_mask].head(300).iterrows():
            issues.append((batch_id, 'before', 'invalid_edu_value', item['row_ref'], 'edu', item['edu'][:1000], '',))

        outlier_mask = standard_df['salary_avg'].notna() & ((standard_df['salary_avg'] < 1000) | (standard_df['salary_avg'] > 100000))
        for _, item in standard_df[outlier_mask].head(300).iterrows():
            issues.append((batch_id, 'before', 'salary_outlier', item['row_ref'], 'salary_avg', str(item['salary_avg']), '',))

        # 6. 执行增强治理（标准化+去重+异常处理）
        after_df = standard_df.copy()
        # 字段标准化
        after_df['city'] = after_df['city'].apply(_normalize_city)
        after_df['edu'] = after_df['edu'].apply(_normalize_edu)
        after_df['workingexp'] = after_df['workingexp'].apply(_normalize_workingexp)

        # 对 salary 解析失败的记录，尝试回填 Excel 的 salary_avg 列
        def _resolve_salary_avg(row_item):
            parsed = row_item.get('salary_avg')
            if parsed is not None and not pd.isna(parsed):
                return parsed
            raw_avg = row_item.get('salary_avg_raw')
            try:
                if raw_avg != '':
                    return float(raw_avg)
            except Exception:
                return None
            return None

        after_df['salary_avg'] = after_df.apply(_resolve_salary_avg, axis=1)

        # 去重（标准化后的键）
        after_df = after_df.drop_duplicates(subset=['job_title', 'company', 'city', 'salary'], keep='first')
        # 剔除明显异常薪资
        after_df = after_df[(after_df['salary_avg'].isna()) | ((after_df['salary_avg'] >= 1000) & (after_df['salary_avg'] <= 100000))]
        # 过滤核心字段缺失
        after_df = after_df[(after_df['job_title'] != '') & (after_df['company'] != '') & (after_df['city'] != '') & (after_df['salary'] != '')]
        after_metrics = calculate_quality_metrics(after_df)
        print(f"🧪 质量快照(after): {after_metrics}")

        # 获取数据库连接
        conn = get_db()
        cursor = conn.cursor()

        # 清空旧数据（可选）
        cursor.execute('TRUNCATE TABLE jobs_51job')
        print("🗑️ 已清空表中旧数据")

        # 保存质量快照与问题明细
        save_quality_snapshot(cursor, batch_id, 'before', before_metrics)
        save_quality_snapshot(cursor, batch_id, 'after', after_metrics)
        save_issue_records(cursor, batch_id, 'before', issues)

        # 准备插入数据
        insert_sql = '''
            INSERT INTO jobs_51job
            (job_title, company, city, salary, salary_avg, company_type,
             company_size, edu, workingexp, job_desc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        # 7. 插入治理后数据
        inserted_count = 0
        failed_rows = []
        for _, row in after_df.iterrows():
            try:
                job_title = str(row['job_title'])[:200]
                company = str(row['company'])[:200]
                city = str(row['city'])[:50]
                salary = str(row['salary'])[:100]
                salary_avg = row['salary_avg']
                company_type = str(row['company_type'])[:100]
                company_size = str(row['company_size'])[:100]
                edu = str(row['edu'])[:50]
                workingexp = str(row['workingexp'])[:50]
                job_desc = str(row['job_desc'])

                cursor.execute(insert_sql, (
                    job_title, company, city, salary, salary_avg,
                    company_type, company_size, edu, workingexp, job_desc
                ))
                inserted_count += 1

                if (inserted_count % 100) == 0:
                    print(f"📈 已插入 {inserted_count} 条数据...")

            except Exception as e:
                err_msg = f"❌ 插入行 {row.get('row_ref', 'unknown')} 数据时出错: {e}"
                print(err_msg)
                failed_rows.append((row.get('row_ref', 'unknown'), err_msg))
                continue

        conn.commit()
        cursor.close()
        conn.close()

        # 6. 打印最终统计
        print(f"\n🎉 数据导入完成！")
        print(f"✅ 成功插入: {inserted_count} 条记录")
        print(f"🧾 质量批次ID: {batch_id}")
        print("📌 可在 data_quality_snapshot / data_quality_issues 查看治理前后结果")
        if failed_rows:
            print(f"❌ 失败行数: {len(failed_rows)} 条（前5条失败记录：{failed_rows[:5]}）")
        return inserted_count

    except Exception as e:
        print(f"\n💥 导入数据总错误: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == '__main__':
    # 测试导入（建议先改为绝对路径，比如：r"C:\project\flinkAnaysis\data\51job.xlsx"）
    # 替换为你的Excel绝对路径！
    excel_abs_path = r"../flinkAnaysis/data/51job.xlsx"  # 改为实际绝对路径
    load_excel_to_mysql(excel_path=excel_abs_path)