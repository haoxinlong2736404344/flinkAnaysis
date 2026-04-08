
"""
初始化数据脚本
用于首次运行时导入 Excel 数据到 MySQL
"""
from database import init_db
from data_loader import load_excel_to_mysql
import os
import traceback


def main():
    """主函数：初始化数据库并导入数据"""
    print("=" * 50)
    print("开始初始化数据库...")
    print("=" * 50)

    # 1. 初始化数据库（创建用户表）
    try:  # 修复：删除多余的 _data 笔误
        print("\n[1/2] 初始化数据库表...")
        init_db()
        print("✓ 数据库表初始化成功！")
    except Exception as e:
        print(f"✗ 数据库初始化失败: {e}")
        traceback.print_exc()  # 打印详细报错栈，便于排查
        return

    # 2. 导入 Excel 数据（优化：转为绝对路径，便于排查）
    excel_path = './data/51job.xlsx'
    excel_abs_path = os.path.abspath(excel_path)  # 转为绝对路径
    if not os.path.exists(excel_abs_path):
        print(f"\n[2/2] 警告: 未找到 Excel 文件（绝对路径）: {excel_abs_path}")
        print("请确保以下路径存在 Excel 文件：")
        print(f"  相对路径: {excel_path}")
        print(f"  绝对路径: {excel_abs_path}")
        return

    try:
        print(f"\n[2/2] 导入 Excel 数据 ({excel_abs_path})...")
        count = load_excel_to_mysql(excel_abs_path)  # 传绝对路径给导入函数
        print(f"✓ 数据导入成功！共导入 {count} 条记录")
    except Exception as e:
        print(f"✗ 数据导入失败: {e}")
        traceback.print_exc()  # 打印详细报错栈
        return

    print("\n" + "=" * 50)
    print("初始化完成！")
    print("=" * 50)
    print("\n默认管理员账号:")
    print("  用户名: admin")
    print("  密码: admin123")
    print("\n现在可以运行 python app.py 启动应用了！")


if __name__ == '__main__':
    main()
