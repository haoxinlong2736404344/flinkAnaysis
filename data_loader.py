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

        # 创建表
        create_jobs_table()

        # 获取数据库连接
        conn = get_db()
        cursor = conn.cursor()

        # 清空旧数据（可选）
        cursor.execute('TRUNCATE TABLE jobs_51job')
        print("🗑️ 已清空表中旧数据")

        # 准备插入数据
        insert_sql = '''
            INSERT INTO jobs_51job 
            (job_title, company, city, salary, salary_avg, company_type, 
             company_size, edu, workingexp, job_desc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

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

        # 插入数据
        inserted_count = 0
        failed_rows = []
        for index, row in df.iterrows():
            try:
                # 5. 取值逻辑优化：匹配不到列时明确赋值为空，打印行号便于排查
                job_title = str(row.get(actual_columns.get('job_title', ''), '')).strip() if actual_columns.get('job_title') else ''
                company = str(row.get(actual_columns.get('company', ''), '')).strip() if actual_columns.get('company') else ''
                city = str(row.get(actual_columns.get('city', ''), '')).strip() if actual_columns.get('city') else ''
                salary = str(row.get(actual_columns.get('salary', ''), '')).strip() if actual_columns.get('salary') else ''
                salary_avg = parse_salary(salary)
                company_type = str(row.get(actual_columns.get('company_type', ''), '')).strip() if actual_columns.get('company_type') else ''
                company_size = str(row.get(actual_columns.get('company_size', ''), '')).strip() if actual_columns.get('company_size') else ''
                edu = str(row.get(actual_columns.get('edu', ''), '')).strip() if actual_columns.get('edu') else ''
                workingexp = str(row.get(actual_columns.get('workingexp', ''), '')).strip() if actual_columns.get('workingexp') else ''
                job_desc = str(row.get(actual_columns.get('job_desc', ''), '')).strip() if actual_columns.get('job_desc') else ''

                # 限制字符串长度
                job_title = job_title[:200]
                company = company[:200]
                city = city[:50]
                salary = salary[:100]
                company_type = company_type[:100]
                company_size = company_size[:100]
                edu = edu[:50]
                workingexp = workingexp[:50]

                cursor.execute(insert_sql, (
                    job_title, company, city, salary, salary_avg,
                    company_type, company_size, edu, workingexp, job_desc
                ))
                inserted_count += 1

                if (inserted_count % 100) == 0:
                    print(f"📈 已插入 {inserted_count} 条数据...")

            except Exception as e:
                err_msg = f"❌ 插入第 {index+1} 行数据时出错: {e}"
                print(err_msg)
                failed_rows.append((index+1, err_msg))
                continue

        conn.commit()
        cursor.close()
        conn.close()

        # 6. 打印最终统计
        print(f"\n🎉 数据导入完成！")
        print(f"✅ 成功插入: {inserted_count} 条记录")
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