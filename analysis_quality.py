"""
数据质量分析模块
用于数据质量总览与治理成效对比
"""
import pymysql
from flink_analysis import get_mysql_connection


METRIC_KEYS = [
    'missing_rate',
    'duplicate_rate',
    'salary_parse_success_rate',
    'edu_valid_rate',
    'outlier_rate'
]


def _safe_float(value):
    if value is None:
        return 0.0
    return round(float(value), 4)


def _safe_int(value):
    if value is None:
        return 0
    return int(value)


def get_latest_batch_id():
    """获取最近一次质量批次ID"""
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT batch_id
            FROM data_quality_snapshot
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        return row['batch_id'] if row else None
    except Exception as e:
        print(f"获取最新批次失败: {e}")
        return None
    finally:
        conn.close()


def _get_snapshot_by_stage(cursor, batch_id, stage):
    cursor.execute(
        """
        SELECT batch_id, stage, total_count, missing_rate, duplicate_rate,
               salary_parse_success_rate, edu_valid_rate, outlier_rate, created_at
        FROM data_quality_snapshot
        WHERE batch_id = %s AND stage = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (batch_id, stage)
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        'batch_id': row['batch_id'],
        'stage': row['stage'],
        'total_count': _safe_int(row['total_count']),
        'missing_rate': _safe_float(row['missing_rate']),
        'duplicate_rate': _safe_float(row['duplicate_rate']),
        'salary_parse_success_rate': _safe_float(row['salary_parse_success_rate']),
        'edu_valid_rate': _safe_float(row['edu_valid_rate']),
        'outlier_rate': _safe_float(row['outlier_rate']),
        'created_at': str(row['created_at']) if row.get('created_at') else ''
    }


def analysis_quality_overview(batch_id=None):
    """获取质量总览（默认最近批次）"""
    if not batch_id:
        batch_id = get_latest_batch_id()
    if not batch_id:
        return {
            'batch_id': '',
            'before': None,
            'after': None,
            'improvement': {},
            'issue_summary': []
        }

    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        before = _get_snapshot_by_stage(cursor, batch_id, 'before')
        after = _get_snapshot_by_stage(cursor, batch_id, 'after')

        improvement = {}
        if before and after:
            for key in METRIC_KEYS:
                delta = round(after[key] - before[key], 4)
                # 对成功率指标，delta>0 是提升；对问题率指标，delta<0 是提升
                if key in ('salary_parse_success_rate', 'edu_valid_rate'):
                    trend = 'improved' if delta > 0 else ('regressed' if delta < 0 else 'unchanged')
                else:
                    trend = 'improved' if delta < 0 else ('regressed' if delta > 0 else 'unchanged')
                improvement[key] = {
                    'before': before[key],
                    'after': after[key],
                    'delta': delta,
                    'trend': trend
                }

        cursor.execute(
            """
            SELECT issue_type, COUNT(*) AS issue_count
            FROM data_quality_issues
            WHERE batch_id = %s
            GROUP BY issue_type
            ORDER BY issue_count DESC
            """,
            (batch_id,)
        )
        issue_summary = [
            {'issue_type': row['issue_type'], 'count': _safe_int(row['issue_count'])}
            for row in cursor.fetchall()
        ]

        return {
            'batch_id': batch_id,
            'before': before,
            'after': after,
            'improvement': improvement,
            'issue_summary': issue_summary
        }
    except Exception as e:
        print(f"获取质量总览失败: {e}")
        return {
            'batch_id': batch_id,
            'before': None,
            'after': None,
            'improvement': {},
            'issue_summary': []
        }
    finally:
        conn.close()


def analysis_quality_compare(limit=12):
    """获取批次级治理成效对比（时间序列）"""
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(
            """
            SELECT DISTINCT batch_id
            FROM data_quality_snapshot
            ORDER BY batch_id DESC
            LIMIT %s
            """,
            (int(limit),)
        )
        # 保持倒序：最新批次优先，便于页面直接看到最新结果
        batch_ids = [row['batch_id'] for row in cursor.fetchall()]

        result = []
        for bid in batch_ids:
            before = _get_snapshot_by_stage(cursor, bid, 'before')
            after = _get_snapshot_by_stage(cursor, bid, 'after')
            if not before or not after:
                continue

            result.append({
                'batch_id': bid,
                'before': before,
                'after': after,
                'improve_missing_rate': round(before['missing_rate'] - after['missing_rate'], 4),
                'improve_duplicate_rate': round(before['duplicate_rate'] - after['duplicate_rate'], 4),
                'improve_salary_parse_success_rate': round(after['salary_parse_success_rate'] - before['salary_parse_success_rate'], 4),
                'improve_edu_valid_rate': round(after['edu_valid_rate'] - before['edu_valid_rate'], 4),
                'improve_outlier_rate': round(before['outlier_rate'] - after['outlier_rate'], 4)
            })

        return result
    except Exception as e:
        print(f"获取质量对比失败: {e}")
        return []
    finally:
        conn.close()
