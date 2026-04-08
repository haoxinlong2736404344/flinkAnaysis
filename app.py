from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from database import init_db, verify_user, add_user, get_all_users, delete_user, update_user, get_user_by_id
from analysis1_city_salary import (analysis_salary_by_city, analysis_city_salary_quantiles,
                                   analysis_province_salary_aggregate, analysis_city_salary_tiers,
                                   analysis_city_comparison, get_all_cities, analysis_all_city_salary)
from analysis2_company_type import analysis_company_type, analysis_company_city_cross, analysis_company_size_cross, analysis_top_company_requirements
from analysis3_education_background import analysis_edu_requirement, analysis_edu_salary, analysis_edu_city_cross, analysis_edu_exp_cross
from analysis4_experience_pay import analysis_exp_salary, analysis_exp_top_cities, analysis_salary_compliance, analysis_popular_jobs_salary_curve
from analysis_quality import analysis_quality_overview, analysis_quality_compare
from analysis_manager import analyze_and_store_results
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key_123'

# 初始化数据库
init_db()

# 不再在启动时自动创建分析表和分析数据
# 改为在使用对应分析模块时按需检查和创建


@app.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('login'))
    return render_template('index.html', user=session['user'])


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = verify_user(username, password)
        if user:
            session['user'] = {'id': user['id'], 'username': user['username'], 'role': user['role']}
            flash('登录成功！', 'success')
            return redirect(url_for('index'))
        else:
            flash('用户名或密码错误！', 'error')
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password2 = request.form['password2']

        if password != password2:
            flash('两次密码不一致！', 'error')
            return render_template('register.html')

        if add_user(username, password):
            flash('注册成功，请登录！', 'success')
            return redirect(url_for('login'))
        else:
            flash('用户名已存在！', 'error')
    return render_template('register.html')


@app.route('/logout')
def logout():
    session.pop('user', None)
    flash('已退出登录', 'success')
    return redirect(url_for('login'))


@app.route('/user_manage')
def user_manage():
    if 'user' not in session:
        return redirect(url_for('login'))
    if session['user']['role'] != 'admin':
        flash('无权限访问！', 'error')
        return redirect(url_for('index'))
    users = get_all_users()
    return render_template('user_manage.html', users=users, user=session['user'])


@app.route('/user_add', methods=['POST'])
def user_add():
    if 'user' not in session or session['user']['role'] != 'admin':
        return redirect(url_for('login'))
    username = request.form['username']
    password = request.form['password']
    role = request.form['role']
    if add_user(username, password, role):
        flash('添加用户成功！', 'success')
    else:
        flash('添加失败，用户名已存在！', 'error')
    return redirect(url_for('user_manage'))


@app.route('/user_delete/<int:user_id>')
def user_delete(user_id):
    if 'user' not in session or session['user']['role'] != 'admin':
        return redirect(url_for('login'))
    delete_user(user_id)
    flash('删除成功！', 'success')
    return redirect(url_for('user_manage'))


@app.route('/user_edit/<int:user_id>', methods=['GET', 'POST'])
def user_edit(user_id):
    if 'user' not in session or session['user']['role'] != 'admin':
        return redirect(url_for('login'))

    if request.method == 'POST':
        username = request.form['username']
        role = request.form['role']
        password = request.form.get('password', '')
        if password:
            update_user(user_id, username, role, password)
        else:
            update_user(user_id, username, role)
        flash('修改成功！', 'success')
        return redirect(url_for('user_manage'))

    edit_user = get_user_by_id(user_id)
    return render_template('user_edit.html', edit_user=edit_user, user=session['user'])


# 四个分析路由
@app.route('/analysis1')
def analysis1():
    if 'user' not in session:
        return redirect(url_for('login'))
    data = analysis_salary_by_city()  # 功能1的数据
    all_cities_data = analysis_all_city_salary()  # 功能6的数据
    return render_template('analysis1.html', data=data, all_cities_data=all_cities_data, user=session['user'])


@app.route('/analysis2')
def analysis2():
    if 'user' not in session:
        return redirect(url_for('login'))
    data = analysis_company_type()
    city_cross_data = analysis_company_city_cross()
    size_cross_data = analysis_company_size_cross()
    edu_requirements_data, exp_requirements_data = analysis_top_company_requirements()
    return render_template('analysis2.html', data=data, city_cross_data=city_cross_data, 
                           size_cross_data=size_cross_data, edu_requirements_data=edu_requirements_data,
                           exp_requirements_data=exp_requirements_data, user=session['user'])

'''
@app.route('/analysis3')
def analysis3():
    if 'user' not in session:
        return redirect(url_for('login'))
    data = analysis_edu_requirement()
    edu_salary_data = analysis_edu_salary()
    edu_exp_data = analysis_edu_exp_cross()
    edu_city_data = analysis_edu_city_cross()
    return render_template('analysis3.html', data=data, edu_salary_data=edu_salary_data, edu_city_data=edu_city_data, edu_exp_data=edu_exp_data, user=session['user'])
'''


@app.route('/analysis3')
def analysis3():
    if 'user' not in session:
        return redirect(url_for('login'))

    # 1. 学历要求分布
    data = analysis_edu_requirement()

    # ——【关键新增：计算占比】——
    total_count = sum(item['count'] for item in data)
    for item in data:
        if total_count > 0:
            item['percent'] = round(item['count'] / total_count * 100, 2)
        else:
            item['percent'] = 0.0
    # ——【新增结束】——

    # 2. 学历 - 薪资
    edu_salary_data = analysis_edu_salary()

    # 3. 学历 - 工作经验
    edu_exp_data = analysis_edu_exp_cross()

    # 4. 学历 - 城市
    edu_city_data = analysis_edu_city_cross()

    return render_template(
        'analysis3.html',
        data=data,
        edu_salary_data=edu_salary_data,
        edu_city_data=edu_city_data,
        edu_exp_data=edu_exp_data,
        user=session['user']
    )


@app.route('/analysis4')
def analysis4():
    if 'user' not in session:
        return redirect(url_for('login'))
    try:
        result = analysis_exp_salary()
        exp_top_cities = analysis_exp_top_cities()
        popular_jobs_data = analysis_popular_jobs_salary_curve()
        return render_template('analysis4.html', data=result, exp_top_cities=exp_top_cities, popular_jobs_data=popular_jobs_data, user=session['user'])
    except Exception as e:
        print(f"分析页面加载失败: {e}")
        return render_template('analysis4.html', data={"chart_data": [], "top_salary_data": {}}, exp_top_cities={}, popular_jobs_data={"job_categories": [], "salary_data": {}}, user=session['user'])


@app.route('/quality')
def quality():
    if 'user' not in session:
        return redirect(url_for('login'))
    overview = analysis_quality_overview()
    return render_template('quality.html', overview=overview, user=session['user'])


@app.route('/quality_effect')
def quality_effect():
    if 'user' not in session:
        return redirect(url_for('login'))
    compare_data = analysis_quality_compare(limit=5)
    return render_template('quality_effect.html', compare_data=compare_data, user=session['user'])


@app.route('/api/quality/overview')
def api_quality_overview():
    if 'user' not in session:
        return jsonify({})
    batch_id = request.args.get('batch_id')
    return jsonify(analysis_quality_overview(batch_id=batch_id))


@app.route('/api/quality/compare')
def api_quality_compare():
    if 'user' not in session:
        return jsonify([])
    limit = request.args.get('limit', 5, type=int)
    return jsonify(analysis_quality_compare(limit=limit if limit and limit > 0 else 5))


@app.route('/api/analysis4/filter', methods=['GET'])
def api_analysis4_filter():
    if 'user' not in session:
        return jsonify({'chart_data': [], 'top_salary_data': {}})
    try:
        # 获取筛选参数
        city = request.args.get('city')
        job_type = request.args.get('job_type')
        edu = request.args.get('edu')

        # 调用分析函数获取数据
        result = analysis_exp_salary(city=city, job_type=job_type, edu=edu)
        return jsonify(result)
    except Exception as e:
        print(f"API分析请求失败: {e}")
        return jsonify({'chart_data': [], 'top_salary_data': {}})


@app.route('/api/analysis4/salary_compliance', methods=['POST'])
def api_salary_compliance():
    if 'user' not in session:
        return jsonify({})
    try:
        data = request.get_json()
        target_salary = data.get('target_salary')
        experience = data.get('experience')
        result = analysis_salary_compliance(target_salary, experience)
        return jsonify(result)
    except Exception as e:
        print(f"薪资达标率计算失败: {e}")
        return jsonify({})
        return jsonify({'chart_data': [], 'top_salary_data': {}})

# API路由：获取所有城市列表（用于多城市对比）
@app.route('/api/cities')
def api_cities():
    if 'user' not in session:
        return jsonify([])
    cities = get_all_cities()
    return jsonify(cities)


@app.route('/api/city_comparison', methods=['POST'])
def api_city_comparison():
    if 'user' not in session:
        return jsonify({'success': False, 'message': '未登录', 'data': []})

    try:
        data = request.get_json()
        selected_cities = data.get('cities', [])

        if not selected_cities:
            return jsonify({'success': False, 'message': '未选择城市', 'data': []})

        result = analysis_city_comparison(selected_cities)

        # 确保返回的数据格式正确
        if not result:
            return jsonify({'success': False, 'message': '未找到数据', 'data': []})

        # 添加success标志
        return jsonify({
            'success': True,
            'message': '获取数据成功',
            'data': result
        })
    except Exception as e:
        print(f"城市对比分析失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}', 'data': []})

# API路由：执行多城市对比（兼容旧路径）
@app.route('/api/compare_cities', methods=['POST'])
def api_compare_cities():
    if 'user' not in session:
        return jsonify([])
    data = request.get_json()
    selected_cities = data.get('cities', [])
    result = analysis_city_comparison(selected_cities)
    return jsonify(result)

# API路由：获取城市薪资分位数数据（功能2）
@app.route('/api/city_quantiles')
def api_city_quantiles():
    if 'user' not in session:
        return jsonify([])
    try:
        # 调用功能2：城市薪资分位数深度分析
        quantile_data = analysis_city_salary_quantiles()

        # 只返回前20个城市的数据，避免图表过于拥挤
        top_quantile_data = quantile_data[:20] if len(quantile_data) > 20 else quantile_data

        return jsonify(top_quantile_data)
    except Exception as e:
        print(f"获取城市分位数数据失败: {e}")
        return jsonify([])


# API路由：获取省级区域薪资聚合数据（功能3）
@app.route('/api/province_analysis')
def api_province_analysis():
    if 'user' not in session:
        return jsonify([])
    try:
        # 调用功能3：省级区域薪资聚合分析
        province_data = analysis_province_salary_aggregate()
        return jsonify(province_data)
    except Exception as e:
        print(f"获取省级分析数据失败: {e}")
        return jsonify([])


# API路由：获取城市薪资梯队划分数据（功能4）
@app.route('/api/city_tiers')
def api_city_tiers():
    if 'user' not in session:
        return jsonify([])
    try:
        # 调用功能4：城市薪资梯队划分与统计
        tier_data = analysis_city_salary_tiers()
        return jsonify(tier_data)
    except Exception as e:
        print(f"获取城市梯队数据失败: {e}")
        return jsonify([])


# API路由：获取特定梯队内的城市详情（功能4的子功能）
# 修改 app.py 中的 /api/tier_cities/<tier_name> 路由
@app.route('/api/tier_cities/<tier_name>')
def api_tier_cities(tier_name):
    if 'user' not in session:
        return jsonify([])
    try:
        # 获取所有城市的分位数数据
        all_cities = analysis_city_salary_quantiles()

        # 根据梯队定义筛选城市 - 使用新的薪资梯队标准
        tier_cities = []
        for city_data in all_cities:
            avg_salary = city_data.get('avg_salary') or city_data.get('q50', 0)

            # 根据梯队定义筛选 - 使用新的标准
            if tier_name == '超高薪' and avg_salary >= 10000:
                tier_cities.append(city_data)
            elif tier_name == '高薪' and 8500 <= avg_salary < 10000:
                tier_cities.append(city_data)
            elif tier_name == '中高薪' and 7000 <= avg_salary < 8500:
                tier_cities.append(city_data)
            elif tier_name == '中等' and 5500 <= avg_salary < 7000:
                tier_cities.append(city_data)
            elif tier_name == '低薪' and avg_salary < 5500:
                tier_cities.append(city_data)

        # 按薪资降序排列（取消限制，返回所有数据）
        tier_cities.sort(key=lambda x: x.get('avg_salary') or x.get('q50', 0), reverse=True)

        # 返回所有城市数据，不再限制数量
        return jsonify(tier_cities)
    except Exception as e:
        print(f"获取{tier_name}梯队城市数据失败: {e}")
        return jsonify([])



if __name__ == '__main__':
    app.run(debug=True, port=5000)