<<<<<<< HEAD
# 招聘数据质量治理与可视化平台（Flink + Flask + MySQL）

一个面向课程设计的招聘数据治理与分析项目，基于 `51job` 数据集，覆盖 **数据质量治理 + 业务分析可视化** 两个方向。  
=======
# 招聘信息大数据分析系统（Flink + Flask + MySQL）
git add README.md
一个面向课程设计的招聘信息分析项目，基于 `51job` 数据集，提供用户登录与管理、城市薪资分析、公司类型分析、学历要求分析、经验与薪资关联分析等可视化功能。  
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
系统支持 **PyFlink 优先** 的分析流程，并在 Flink 不可用时自动降级为 **MySQL 直查**，保证可运行性。

---

<<<<<<< HEAD
## 1. 项目特性

- 基于 Flask 的 Web 平台，包含登录注册与管理员用户管理。
=======
1. 项目特性

- 基于 Flask 的 Web 分析平台，包含登录注册与管理员用户管理。
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
- 以 MySQL 存储原始岗位数据与分析结果，支持按需建表与缓存结果复用。
- 分析引擎支持双模式：
  - Flink 可用时：使用 PyFlink 进行分析；
  - Flink 不可用时：自动切换到 MySQL 聚合查询。
<<<<<<< HEAD
- 提供 **数据治理看板 + 4 大业务分析模块**：
  - 数据质量总览与治理成效对比（新增）
=======
- 提供 4 大分析模块与多个 API：
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
  - 城市薪资分布/分位数/梯队/城市对比
  - 公司类型分布与交叉分析
  - 学历要求分布与交叉分析
  - 工作经验与薪资关系（含筛选、达标率、热门岗位薪资曲线）

---

<<<<<<< HEAD
## 2. 技术栈
=======
2. 技术栈
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 后端：`Flask`
- 数据分析：`PyFlink`（可选） + `MySQL SQL`
- 数据库驱动：`PyMySQL`
- 数据处理：`pandas`、`openpyxl`
- 前端：Jinja2 模板 + 静态 CSS

核心依赖见 `requirements.txt`：

- `flask==2.3.3`
- `apache-flink==1.17.1`
- `PyMySQL==1.1.0`
- `pandas==1.3.5`

---

<<<<<<< HEAD
## 3. 项目结构
=======
3. 项目结构
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

```text
flinkAnaysis/
├─ app.py                             # Flask 入口与路由
├─ config.py                          # MySQL/Flink 配置、USE_FLINK 开关
├─ database.py                        # 用户与数据库初始化
├─ data_loader.py                     # Excel -> MySQL 导入
├─ init_data.py                       # 一键初始化脚本
├─ flink_analysis.py                  # Flink 环境探测与连接封装
├─ analysis_manager.py                # 分析结果表管理、分析写回
<<<<<<< HEAD
├─ analysis_quality.py                # 数据质量总览/成效对比分析（新增）
=======
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
├─ analysis1_city_salary.py           # 城市薪资分析模块
├─ analysis2_company_type.py          # 公司类型分析模块
├─ analysis3_education_background.py  # 学历分析模块
├─ analysis4_experience_pay.py        # 经验-薪资分析模块
├─ templates/                         # 页面模板
├─ static/                            # 静态资源
└─ data/51job.xlsx                    # 原始招聘数据（初始化依赖）
```

---

<<<<<<< HEAD
## 4. 数据库设计（核心表）

### 4.1 基础业务表
=======
4. 数据库设计（核心表）

4.1 基础业务表
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- `users`：系统用户（登录/权限）
- `jobs_51job`：招聘原始数据（职位、公司、城市、薪资、学历、经验等）

<<<<<<< HEAD
### 4.2 分析结果缓存表（按需创建）
=======
4.2 分析结果缓存表（按需创建）
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- `CitySalaryDistribution`
- `CompanyTypeDistribution`
- `EducationRequirementAnalysis`
- `ExperienceSalaryRelation`

<<<<<<< HEAD
### 4.3 数据治理相关表（新增）

- `data_quality_snapshot`：质量指标快照（before/after）
  - 核心字段：`batch_id`、`stage`、`missing_rate`、`duplicate_rate`、`salary_parse_success_rate`、`edu_valid_rate`、`outlier_rate`
- `data_quality_issues`：质量问题明细
  - 核心字段：`batch_id`、`issue_type`、`row_ref`、`field_name`、`raw_value`、`fix_value`

=======
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
说明：
- 访问分析页面时会先检查结果表是否存在及是否有数据；
- 若无数据则触发分析并写入结果表；
- 后续访问可直接读取缓存结果，减少重复计算。
<<<<<<< HEAD
- 每次执行 `init_data.py` 导入时，会自动生成一组 `batch_id` 对应的治理前后质量快照。

---

## 5. 运行前准备

## 5.1 环境要求
=======

---

5. 运行前准备

5.1 环境要求
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- Python 3.9+（建议）
- MySQL 8.x（5.7 理论可运行，建议 8.x）
- （可选）本地可用的 Flink/PyFlink 环境

<<<<<<< HEAD
## 5.2 配置文件
=======
5.2 配置文件
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

编辑 `config.py`（或通过环境变量覆盖）：

- `MYSQL_HOST`（默认 `localhost`）
- `MYSQL_PORT`（默认 `3306`）
- `MYSQL_USER`（默认 `root`）
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`（默认 `flink_analysis`）
- `USE_FLINK`（`True/False`，控制是否尝试 Flink）

> 建议将数据库密码放到环境变量中，不要在代码中明文保存。

---

<<<<<<< HEAD
## 6. 安装与启动

### 6.1 安装依赖
=======
6. 安装与启动

6.1 安装依赖
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

```bash
pip install -r requirements.txt
```

<<<<<<< HEAD
### 6.2 初始化数据库与导入 Excel
=======
6.2 初始化数据库与导入 Excel
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

```bash
python init_data.py
```

初始化内容：
- 自动创建数据库（若不存在）；
- 创建 `users` 表并写入默认管理员；
<<<<<<< HEAD
- 读取 `data/51job.xlsx`，清空并重建 `jobs_51job` 数据；
- 自动生成数据质量治理快照（before/after）及问题明细（新增）。

### 6.3 启动项目
=======
- 读取 `data/51job.xlsx`，清空并重建 `jobs_51job` 数据。

6.3 启动项目
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

```bash
python app.py
```

默认访问地址：`http://127.0.0.1:5000`

默认管理员账号：
- 用户名：`admin`
- 密码：`admin123`

---

<<<<<<< HEAD
## 7. 功能说明

## 7.1 用户与权限
=======
7. 功能说明

7.1 用户与权限
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 登录、注册、退出登录；
- 管理员可访问用户管理：新增/编辑/删除用户；
- 非管理员访问管理页面会被拦截。

<<<<<<< HEAD
## 7.2 分析模块

### 模块0：数据质量治理（`/quality`、`/quality_effect`，新增）

- 最新批次质量总览（治理前后指标对比）；
- 问题类型分布（缺失/重复/薪资解析失败/学历非法/异常值）；
- 多批次治理成效趋势（按批次展示提升幅度）；
- 批次明细表支持报告截图与答辩展示。

### 模块一：城市薪资分析（`/analysis1`）
=======
7.2 分析模块

模块一：城市薪资分析（`/analysis1`）
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 各城市平均薪资与岗位数量；
- 城市薪资分位数分析（Q25/Q50/Q75/Q90）；
- 城市薪资梯队划分；
- 省级薪资聚合；
- 多城市薪资对比；
- 全量城市薪资与岗位数量展示。

<<<<<<< HEAD
### 模块二：公司类型分析（`/analysis2`）
=======
模块二：公司类型分析（`/analysis2`）
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 公司类型分布；
- 公司类型-城市交叉分析；
- 公司类型-公司规模交叉分析；
- TOP5 公司类型的学历/经验要求分析。

<<<<<<< HEAD
### 模块三：学历分析（`/analysis3`）
=======
模块三：学历分析（`/analysis3`）
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 学历要求分布（页面中计算并展示百分比）；
- 学历-薪资关联（均值/中位数/极值）；
- 学历-经验交叉分析；
- 学历-城市交叉分析。

<<<<<<< HEAD
### 模块四：经验与薪资分析（`/analysis4`）
=======
模块四：经验与薪资分析（`/analysis4`）
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 经验段平均薪资与岗位数；
- 支持城市/岗位关键词/学历筛选；
- 薪资分布统计（分位数、标准差、变异系数等）；
- 薪资达标率计算；
- 热门岗位薪资曲线；
- 各经验段高薪城市统计。

---

<<<<<<< HEAD
## 8. 主要 API（节选）

- `GET /api/quality/overview`：质量总览（支持 `batch_id`）
- `GET /api/quality/compare`：治理成效对比（支持 `limit`）
=======
8. 主要 API（节选）

>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c
- `GET /api/cities`：获取城市列表
- `POST /api/city_comparison`：多城市对比
- `POST /api/compare_cities`：城市对比兼容接口
- `GET /api/city_quantiles`：城市薪资分位数
- `GET /api/province_analysis`：省级聚合
- `GET /api/city_tiers`：城市薪资梯队
- `GET /api/tier_cities/<tier_name>`：梯队城市详情
- `GET /api/analysis4/filter`：分析4筛选查询
- `POST /api/analysis4/salary_compliance`：薪资达标率

---

<<<<<<< HEAD
## 9. Flink 运行逻辑
=======
9. Flink 运行逻辑
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 启动分析时会调用 `is_flink_available()` 检测 Flink 环境。
- `USE_FLINK=False` 时直接走 MySQL 查询方案。
- Flink 执行异常时自动回退到 MySQL（代码内已做 try/catch 兜底）。
- 对课程设计场景：即使 Flink 环境缺失，系统核心分析仍可运行。

---

<<<<<<< HEAD
## 10. 常见问题

### Q1：`init_data.py` 报找不到 Excel 文件
=======
10. 常见问题

Q1：`init_data.py` 报找不到 Excel 文件
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

确认文件存在：`data/51job.xlsx`。  
脚本会打印绝对路径，可按提示修正。

<<<<<<< HEAD
### Q2：Flink 初始化失败怎么办？
=======
Q2：Flink 初始化失败怎么办？
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 将 `config.py` 中 `USE_FLINK=False`；
- 继续使用 MySQL 分析路径即可。

<<<<<<< HEAD
### Q3：页面空白或分析无数据
=======
Q3：页面空白或分析无数据
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 先确认 `jobs_51job` 是否成功导入；
- 再访问对应分析页触发结果表计算；
- 检查 MySQL 连接与权限配置。

<<<<<<< HEAD
### Q4：质量看板没有数据

- 请先执行 `python init_data.py` 触发导入与质量快照生成；
- 确认 `data_quality_snapshot` 表中已有 `before/after` 记录；
- 再访问 `/quality` 与 `/quality_effect`。

---

## 11. 安全与改进建议
=======
---

11. 安全与改进建议
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

- 当前默认管理员和数据库密码配置较弱，建议立即修改。
- 用户密码目前为 MD5，建议升级为带盐哈希（如 `bcrypt`）。
- 部分查询为字符串拼接，建议统一改为参数化，降低注入风险。
- 建议补充：
  - `.env` 配置加载；
  - 单元测试/接口测试；
  - Docker 一键部署（MySQL + Web）。

---

<<<<<<< HEAD
## 12. 开发建议流程

1. 准备 MySQL 并配置 `config.py` 或环境变量。  
2. 执行 `python init_data.py` 导入数据。  
3. 检查质量快照表：`data_quality_snapshot`、`data_quality_issues`。  
4. 执行 `python app.py` 启动服务。  
5. 使用管理员账号登录，先看 `/quality`、`/quality_effect`，再验证业务分析页面。  
6. 若 Flink 报错，关闭 `USE_FLINK` 后重试。

---

## 13. 课程答辩建议（可直接使用）

- **给谁看**：HR总监/招聘经理/数据运营。  
- **解决什么问题**：招聘数据质量不稳定导致分析结论不可靠。  
- **怎么解决**：
  1. 建立质量指标（缺失率、重复率、解析成功率、合法率、异常率）；
  2. 导入时自动治理与留痕；
  3. 可视化治理前后效果 + 业务分析结果。  
- **成果展示**：`/quality` 展示治理前后对比，`/quality_effect` 展示多批次趋势。

---

## 14. 许可
=======
12. 开发建议流程

1. 准备 MySQL 并配置 `config.py` 或环境变量。  
2. 执行 `python init_data.py` 导入数据。  
3. 执行 `python app.py` 启动服务。  
4. 使用管理员账号登录，依次访问分析页面验证结果。  
5. 若 Flink 报错，关闭 `USE_FLINK` 后重试。

---

13. 许可
>>>>>>> d0b332075dc9187256dd72ed4cd3373e0034168c

该项目用于教学/课程设计场景，具体开源许可请按你的课程或仓库要求补充（如 MIT）。

