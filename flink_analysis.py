"""
使用 PyFlink 连接 MySQL 进行数据分析
如果 Flink 不可用，自动使用 MySQL 直接查询（备用方案）
"""
import pymysql
from config import FLINK_MYSQL_CONFIG, MYSQL_CONFIG, USE_FLINK
import subprocess
import sys

# 全局标志：Flink 是否可用
_flink_available = None




def is_flink_available():
    """检查 Flink 是否可用"""
    global _flink_available

    if _flink_available is not None:
        return _flink_available

    # 如果配置中禁用了 Flink，直接返回 False
    if not USE_FLINK:
        _flink_available = False
        print("提示: Flink 已在配置中禁用，使用 MySQL 直接查询")
        return False


    # 尝试初始化 Flink（只测试一次）
    try:
        from pyflink.table import EnvironmentSettings, TableEnvironment
        env_settings = EnvironmentSettings.in_batch_mode()
        t_env = TableEnvironment.create(env_settings)
        _flink_available = True
        print("提示: Flink 环境初始化成功")
        return True
    except Exception as e:
        error_msg = str(e)
        print(f"提示: Flink 初始化失败，将使用 MySQL 直接查询: {error_msg[:100]}")
        _flink_available = False
        return False


def get_flink_table_env():
    """创建并配置 Flink Table 环境"""
    from pyflink.table import EnvironmentSettings, TableEnvironment

    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # 注册 MySQL JDBC 连接器
    mysql_ddl = f"""
    CREATE TABLE jobs_51job_source (
        id INT,
        job_title STRING,
        company STRING,
        city STRING,
        salary STRING,
        salary_avg DECIMAL(10, 2),
        company_type STRING,
        company_size STRING,
        edu STRING,
        workingexp STRING,
        job_desc STRING,
        created_at TIMESTAMP
    ) WITH (
        'connector' = 'jdbc',
       'url' = 'jdbc:mysql://{FLINK_MYSQL_CONFIG["hostname"]}:{FLINK_MYSQL_CONFIG["port"]}/{FLINK_MYSQL_CONFIG["database-name"]}?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true',
        'table-name' = 'jobs_51job',
        'username' = '{FLINK_MYSQL_CONFIG['username']}',
        'password' = '{FLINK_MYSQL_CONFIG['password']}',
        'driver' = 'com.mysql.cj.jdbc.Driver'
    )
    """

    t_env.execute_sql(mysql_ddl)
    return t_env


def get_mysql_connection():
    """获取 MySQL 连接（备用方案）"""
    return pymysql.connect(
        host=MYSQL_CONFIG['host'],
        port=MYSQL_CONFIG['port'],
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password'],
        database=MYSQL_CONFIG['database'],
        charset=MYSQL_CONFIG['charset'],
        cursorclass=pymysql.cursors.DictCursor
    )



