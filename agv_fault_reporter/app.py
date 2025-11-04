import sqlite3
import csv
from flask import Flask, render_template, request, redirect, url_for, g, Response, flash
from datetime import datetime
import io
import re # 导入正则表达式模块
import json
# 1. 导入 Limiter 相关的模块
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import math

app = Flask(__name__)
app.secret_key = 'your_very_secret_key' # flash消息需要一个密钥
DATABASE = 'faults.db'

# 2. 初始化 Limiter
limiter = Limiter(
    app,
    key_func=get_remote_address,  # 使用 IP 地址作为识别用户的依据
    default_limits=["200 per day", "50 per hour"] # 为所有路由设置一个默认的全局限制
)

# --- 全局常量定义 ---
FAULT_CATEGORIES = ["充电失败", "任务执行失败", "避障异常", "定位丢失", "机械故障", "其他"]
FAULT_STATUSES = ["待修复", "处理中", "已修复"]

# --- 数据库管理 (无变动) ---
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

# --- 核心解析与业务逻辑 ---
def parse_fault_text(raw_text):
    """从原始文本中解析故障信息字段"""
    data = {}
    # 使用正则表达式匹配键值对，注意处理冒号的全角和半角
    pattern = re.compile(r'^(发现人员|时间|车辆信息|报警描述|解决办法|责任人)[:：]\s*(.*)', re.MULTILINE)
    matches = dict(pattern.findall(raw_text))

    # 1. 提取基本字段
    data['reporter_name'] = matches.get('发现人员', '').strip()
    data['vehicle_id'] = matches.get('车辆信息', '').strip()
    data['description'] = matches.get('报警描述', '').strip()
    data['solution'] = matches.get('解决办法', '').strip()

    responsible_person_raw = matches.get('责任人', '').strip()

    # --- 关键修改：数据清洗 ---
    # 定义一个正则表达式，只保留：中文字符、大小写字母、数字、空格和@符号
    # \u4e00-\u9fa5 是中文字符的Unicode范围
    clean_pattern = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9\s@]+')
    # 将所有匹配到的乱码（如 ￳...￰）替换成一个空格。
    responsible_person_cleaned = clean_pattern.sub(' ', responsible_person_raw).strip()

    # 清洗后再去除可能存在的@前缀（虽然我们的正则已经很宽容了）
    data['responsible_person'] = responsible_person_cleaned.lstrip('@')


    # 2. 解析时间字段 (支持多种格式)
    time_str = matches.get('时间', '').strip()
    try:
        # 格式: 2025年10月29日15：53
        data['fault_time'] = datetime.strptime(time_str.replace('：', ':'), '%Y年%m月%d日%H:%M')
    except ValueError:
        data['fault_time'] = None # 解析失败

    # 3. 根据描述生成错误类别
    desc = data['description']
    if '充电' in desc:
        data['category'] = '充电失败'
    elif '避障' in desc:
        data['category'] = '避障异常'
    elif '定位' in desc:
        data['category'] = '定位丢失'
    elif '任务' in desc:
        data['category'] = '任务执行失败'
    else:
        data['category'] = '其他'

    return data

# --- 视图函数 ---

@app.route('/', methods=['GET', 'POST'])
# 3. 为提交操作应用特定的速率限制
@limiter.limit("10 per minute", methods=['POST'])
def index():
    db = get_db()
    # “详细上报”表单的提交逻辑
    if request.method == 'POST':
        try:
            reporter_name = request.form['reporter_name']
            fault_time_str = request.form['fault_time']
            fault_time = datetime.strptime(fault_time_str, '%Y-%m-%dT%H:%M')
            vehicle_id = request.form['vehicle_id']
            category = request.form['category']
            description = request.form['description']
            solution = request.form['solution']
            responsible_person = request.form['responsible_person']

            db.execute(
                'INSERT INTO faults (reporter_name, fault_time, vehicle_id, category, description, solution, responsible_person) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (reporter_name, fault_time, vehicle_id, category, description, solution, responsible_person)
            )
            db.commit()
            flash('故障已成功提交！', 'success')
        except Exception as e:
            flash(f'提交失败: {e}', 'error')
        # 关键修改1：重定向时，指明要激活 'detailed' tab
        return redirect(url_for('index', tab='detailed'))

    # --- 搜索与筛选逻辑 ---
    search_params = {
        'search_reporter': request.args.get('search_reporter', '').strip(),
        'search_responsible': request.args.get('search_responsible', '').strip(),
        'search_vehicle': request.args.get('search_vehicle', '').strip(),
        'search_status': request.args.get('search_status', '').strip(),
        'search_start_date': request.args.get('search_start_date', '').strip(),
        'search_end_date': request.args.get('search_end_date', '').strip()
    }

    where_clauses = ["1=1"]
    params = []

    if search_params['search_reporter']:
        where_clauses.append("reporter_name LIKE ?")
        params.append(f"%{search_params['search_reporter']}%")
    if search_params['search_responsible']:
        where_clauses.append("responsible_person LIKE ?")
        params.append(f"%{search_params['search_responsible']}%")
    if search_params['search_vehicle']:
        where_clauses.append("vehicle_id LIKE ?")
        params.append(f"%{search_params['search_vehicle']}%")
    if search_params['search_status']:
        where_clauses.append("status = ?")
        params.append(search_params['search_status'])
    if search_params['search_start_date']:
        where_clauses.append("fault_time >= ?")
        params.append(datetime.strptime(search_params['search_start_date'], '%Y-%m-%d'))
    if search_params['search_end_date']:
        end_date = datetime.strptime(search_params['search_end_date'], '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        where_clauses.append("fault_time <= ?")
        params.append(end_date)

    where_sql = " AND ".join(where_clauses)

    # --- 分页逻辑开始 ---
    # 1. 定义允许的 per_page 值白名单
    allowed_per_page = [4, 5, 10]
    
    # 2. 获取 per_page 参数，验证并设置默认值
    per_page = request.args.get('per_page', 5, type=int)
    if per_page not in allowed_per_page:
        per_page = 5 # 如果传入了无效值，强制设为默认值

    # 3. 获取当前页码
    page = request.args.get('page', 1, type=int)

    # 4. 查询总数 (必须使用同样的 WHERE 子句)
    total_count_query = f"SELECT COUNT(id) FROM faults WHERE {where_sql}"
    total_count = db.execute(total_count_query, params).fetchone()[0]

    # 5. 使用动态的 per_page 计算总页数
    total_pages = math.ceil(total_count / per_page)
    
    # 确保 page 不会超出范围
    if page > total_pages and total_pages > 0:
        page = total_pages

    # 6. 计算 offset
    offset = (page - 1) * per_page
    
    # 获取当页数据 (使用同样的 WHERE 子句)
    faults_query = f"SELECT * FROM faults WHERE {where_sql} ORDER BY fault_time DESC LIMIT ? OFFSET ?"
    if app.config.get('DEBUG_SQL'):
        print("\n--- DEBUG SQL (Fetch Page Data) ---")
        print("Query:", faults_query)
        print("Params:", params + [per_page, offset])
        print("---------------------------------\n")
    
    faults = db.execute(faults_query, params + [per_page, offset]).fetchall()

    active_tab = 'quick'
    
    # 8. 将 per_page 也传递给模板
    return render_template(
        'index.html', 
        faults=faults, 
        categories=FAULT_CATEGORIES,
        statuses=FAULT_STATUSES, # 将状态列表也传给前端
        active_tab=active_tab,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        allowed_per_page=allowed_per_page,
        search_params=search_params # 将搜索参数传回，用于填充表单
    )

# 新增：“快速解析”的路由
@app.route('/parse', methods=['POST'])
@limiter.limit("10 per minute")
def parse_fault():
    raw_text = request.form.get('raw_text', '')
    parsed_data = parse_fault_text(raw_text)

    # 检查关键字段是否解析成功
    required_fields = ['reporter_name', 'fault_time', 'vehicle_id', 'description', 'responsible_person']
    if not all(parsed_data.get(field) for field in required_fields):
        flash('解析失败！请检查输入格式是否正确，确保包含所有必需字段。', 'error')
        # 关键修改4：如果解析失败，也要告诉首页激活 'quick' tab
        return redirect(url_for('index', tab='quick'))

    try:
        db = get_db()
        db.execute(
            'INSERT INTO faults (reporter_name, fault_time, vehicle_id, category, description, solution, responsible_person) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (
                parsed_data['reporter_name'],
                parsed_data['fault_time'],
                parsed_data['vehicle_id'],
                parsed_data['category'],
                parsed_data['description'],
                parsed_data['solution'],
                parsed_data['responsible_person']
            )
        )
        db.commit()
        flash('通过快速解析成功提交故障！', 'success')
    except Exception as e:
        flash(f'数据库插入失败: {e}', 'error')

    # 关键修改5：成功提交后，告诉首页激活 'quick' tab
    return redirect(url_for('index', tab='quick'))


# (edit_fault, statistics, download 等函数保持不变)
@app.route('/edit/<int:fault_id>', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def edit_fault(fault_id):
    db = get_db()
    if request.method == 'POST':
        status = request.form['status']
        resolution_log = request.form['resolution_log']
        db.execute('UPDATE faults SET status = ?, resolution_log = ? WHERE id = ?', (status, resolution_log, fault_id))
        db.commit()
        flash('记录已更新！', 'success')
        return redirect(url_for('index'))
    fault = db.execute('SELECT * FROM faults WHERE id = ?', (fault_id,)).fetchone()
    if fault is None: return "Fault not found", 404
    return render_template('edit.html', fault=fault, statuses=FAULT_STATUSES)

@app.route('/statistics')
def statistics():
    db = get_db()
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    
    # 安全加固
    # 1. 定义一个允许用于分组的列名白名单
    allowed_group_by_columns = ['category', 'status', 'vehicle_id', 'reporter_name', 'responsible_person', 'by_date']
    
    # 2. 从请求中获取 group_by 参数，默认为 'category'
    group_by = request.args.get('group_by', 'category')
    
    # 3. 验证参数是否在白名单内，如果不在，则强制使用默认值
    if group_by not in allowed_group_by_columns:
        group_by = 'category'
        flash('检测到无效的统计维度，已重置为默认值。', 'warning')

    # 根据 group_by 参数构建查询
    group_by_clause = ""
    if group_by == 'by_date':
        # 如果是按天统计，则使用 DATE 函数
        group_by_clause = "DATE(fault_time)"
    else:
        # 其他情况直接使用列名
        group_by_clause = group_by

    # 4. 现在可以安全地在 f-string 中使用 group_by 变量
    query = f"SELECT {group_by_clause} as group_key, COUNT(*) as count FROM faults WHERE 1=1"
    params = []
    if start_date_str:
        query += " AND fault_time >= ?"
        params.append(datetime.strptime(start_date_str, '%Y-%m-%d'))
    if end_date_str:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query += " AND fault_time <= ?"
        params.append(end_date)

    if group_by == 'by_date':
        query += f" GROUP BY group_key ORDER BY group_key ASC" # 按天统计时，按日期升序排列
    else:
        query += f" GROUP BY {group_by} ORDER BY count DESC"    # 按数量降序排序

    cursor = db.execute(query, params)
    stats = cursor.fetchall()

    # --- 关键修改：为图表准备数据 ---
    chart_labels = []
    chart_data = []
    if stats:
        chart_labels = [row['group_key'] for row in stats]
        chart_data = [row['count'] for row in stats]

    return render_template(
        'statistics.html',
        stats=stats,
        current_group_by=group_by,
        start_date=start_date_str,
        end_date=end_date_str,
        # 将图表数据传递给模板，并使用 tojson 过滤器确保安全
        chart_labels=json.dumps(chart_labels),
        chart_data=json.dumps(chart_data)
    )

@app.route('/download')
@limiter.limit("5 per minute")
def download():
    db = get_db()
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    query = "SELECT id, reporter_name, fault_time, vehicle_id, category, status, description, solution, resolution_log, responsible_person FROM faults WHERE 1=1"
    params = []
    if start_date_str:
        query += " AND fault_time >= ?"
        params.append(datetime.strptime(start_date_str, '%Y-%m-%d'))
    if end_date_str:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        query += " AND fault_time <= ?"
        params.append(end_date)
    query += " ORDER BY fault_time DESC"
    faults_to_download = db.execute(query, params).fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', '发现人员', '故障时间', '车辆信息', '错误类别', '解决状态', '报警描述', '解决办法', '处理记录', '责任人'])
    for row in faults_to_download: writer.writerow(row)
    csv_content = output.getvalue()
    encoded_content = csv_content.encode('utf-8-sig')
    return Response(encoded_content, mimetype="text/csv", headers={"Content-Disposition": f"attachment;filename=agv_faults_{datetime.now().strftime('%Y%m%d')}.csv"})

if __name__ == '__main__':
    # 只有在直接运行时，才设置这个配置项为 True
    app.config['DEBUG_SQL'] = True
    print("SQL debugging is ON. Running in development mode.")
    app.run(host='0.0.0.0', port=5000, debug=True)
