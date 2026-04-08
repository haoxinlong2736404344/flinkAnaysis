"""
数据库操作模块 - 使用 MySQL
"""
import pymysql
import hashlib
from config import MYSQL_CONFIG


def get_db():
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            charset=MYSQL_CONFIG['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
    except Exception as e:
        print(f"数据库连接错误: {e}")
        raise


def init_db():
    """初始化数据库，创建用户表"""
    try:
        # 先连接到 MySQL 服务器（不指定数据库）
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            charset=MYSQL_CONFIG['charset']
        )
        cursor = conn.cursor()
        
        # 创建数据库（如果不存在）
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.commit()
        cursor.close()
        conn.close()
        
        # 连接到指定数据库
        conn = get_db()
        cursor = conn.cursor()
        
        # 创建用户表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(20) DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        # 创建默认管理员（如果不存在）
        admin_pwd = hashlib.md5('admin123'.encode()).hexdigest()
        cursor.execute('SELECT id FROM users WHERE username = %s', ('admin',))
        if not cursor.fetchone():
            cursor.execute(
                'INSERT INTO users (username, password, role) VALUES (%s, %s, %s)',
                ('admin', admin_pwd, 'admin')
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("数据库初始化成功！")
    except Exception as e:
        print(f"数据库初始化错误: {e}")
        raise


def verify_user(username, password):
    """验证用户登录"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        pwd_hash = hashlib.md5(password.encode()).hexdigest()
        cursor.execute(
            'SELECT * FROM users WHERE username = %s AND password = %s',
            (username, pwd_hash)
        )
        user = cursor.fetchone()
        return user
    except Exception as e:
        print(f"验证用户错误: {e}")
        return None
    finally:
        conn.close()


def add_user(username, password, role='user'):
    """添加新用户"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        pwd_hash = hashlib.md5(password.encode()).hexdigest()
        cursor.execute(
            'INSERT INTO users (username, password, role) VALUES (%s, %s, %s)',
            (username, pwd_hash, role)
        )
        conn.commit()
        return True
    except pymysql.IntegrityError:
        # 用户名已存在
        return False
    except Exception as e:
        print(f"添加用户错误: {e}")
        return False
    finally:
        conn.close()


def get_all_users():
    """获取所有用户"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT id, username, role, created_at FROM users')
        users = cursor.fetchall()
        return users
    except Exception as e:
        print(f"获取用户列表错误: {e}")
        return []
    finally:
        conn.close()


def delete_user(user_id):
    """删除用户"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
    except Exception as e:
        print(f"删除用户错误: {e}")
    finally:
        conn.close()


def update_user(user_id, username, role, password=None):
    """更新用户信息"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        if password:
            pwd_hash = hashlib.md5(password.encode()).hexdigest()
            cursor.execute(
                'UPDATE users SET username = %s, role = %s, password = %s WHERE id = %s',
                (username, role, pwd_hash, user_id)
            )
        else:
            cursor.execute(
                'UPDATE users SET username = %s, role = %s WHERE id = %s',
                (username, role, user_id)
            )
        conn.commit()
    except Exception as e:
        print(f"更新用户错误: {e}")
    finally:
        conn.close()


def get_user_by_id(user_id):
    """根据ID获取用户"""
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        user = cursor.fetchone()
        return user
    except Exception as e:
        print(f"获取用户错误: {e}")
        return None
    finally:
        conn.close()
