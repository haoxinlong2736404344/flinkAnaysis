"""
数据库配置文件
"""
import os

# MySQL 数据库配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'hxl123..'),
    'database': os.getenv('MYSQL_DATABASE', 'flink_analysis'),
    'charset': 'utf8mb4'
}

# Flink MySQL 连接器配置
FLINK_MYSQL_CONFIG = {
    'hostname': os.getenv('MYSQL_HOST', 'localhost'),
    'port': os.getenv('MYSQL_PORT', '3306'),
    'username': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'hxl123..'),
    'database-name': os.getenv('MYSQL_DATABASE', 'flink_analysis')
}

# 是否使用 Flink（如果 Java 版本不够或 Flink 不可用，设置为 False）
# 设置为 False 时，直接使用 MySQL 查询，性能同样优秀
USE_FLINK = True

