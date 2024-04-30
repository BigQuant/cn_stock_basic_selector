"""cn_stock_basic_selector package.

A股-基础选股
"""
import uuid
from collections import OrderedDict

import structlog

from bigmodule import I

logger = structlog.get_logger()

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "选股"
# 模块显示名
friendly_name = "A股-基础选股"
# 回测
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-HVwrgP4J1A#h-a股-基础选股3"
# 是否自动缓存结果
cacheable = True

EXCHANGES = OrderedDict([
    ("上交所", "cn_stock_bar1d.instrument LIKE '%.SH'"),
    ("深交所", "cn_stock_bar1d.instrument LIKE '%.SZ'"),
    ("北交所", "cn_stock_bar1d.instrument LIKE '%.BJ'"),
])

LIST_SECTORS = OrderedDict([
    ("主板", 1),
    ("创业板", 2),
    ("科创板", 3),
    ("北交所", 4),
])

INDEXES = OrderedDict([
    ("中证500", "cn_stock_factors_base.is_zz500 == 1"),
    ("上证指数", "cn_stock_factors_base.is_szzs == 1"),
    ("创业板指", "cn_stock_factors_base.is_cybz == 1"),
    ("深证成指", "cn_stock_factors_base.is_szcz == 1"),
    ("北证50", "cn_stock_factors_base.is_bz50 == 1"),
    ("上证50", "cn_stock_factors_base.is_sh50 == 1"),
    ("科创50", "cn_stock_factors_base.is_kc50 == 1"),
    ("沪深300", "cn_stock_factors_base.is_hs300 == 1"),
    ("中证1000", "cn_stock_factors_base.is_zz1000 == 1"),
    ("中证100", "cn_stock_factors_base.is_zz100 == 1"),
    ("深证100", "cn_stock_factors_base.is_sz100 == 1"),
])

ST_STATUSES = OrderedDict([
    ("正常", 0),
    ("ST", 1),
    ("*ST", 2),
])

MARGIN_TRADINGS = OrderedDict([
    ("两融标的", 1),
    ("非两融标的", 0),
])

SW2021_INDUSTRIES = OrderedDict([
    ("农林牧渔", "110000"),
    ("采掘", "210000"),
    ("基础化工", "220000"),
    ("钢铁", "230000"),
    ("有色金属", "240000"),
    ("建筑建材", "250000"),
    ("机械设备", "640000"),
    ("电子", "270000"),
    ("汽车", "280000"),
    ("交运设备", "310000"),
    ("信息设备", "320000"),
    ("家用电器", "330000"),
    ("食品饮料", "340000"),
    ("纺织服饰", "350000"),
    ("轻工制造", "360000"),
    ("医药生物", "370000"),
    ("公用事业", "410000"),
    ("交通运输", "420000"),
    ("房地产", "430000"),
    ("金融服务", "440000"),
    ("商贸零售", "450000"),
    ("社会服务", "460000"),
    ("信息服务", "470000"),
    ("银行", "480000"),
    ("非银金融", "490000"),
    ("综合", "510000"),
    ("建筑材料", "610000"),
    ("建筑装饰", "620000"),
    ("电力设备", "630000"),
    ("国防军工", "650000"),
    ("计算机", "710000"),
    ("传媒", "720000"),
    ("通信", "730000"),
    ("煤炭", "740000"),
    ("石油石化", "750000"),
    ("环保", "760000"),
    ("美容护理", "770000"),
])

SQL_TEMPLATE = '''
SELECT
    date,
    instrument
FROM
    {tables}
WHERE
    {where_filters}
QUALIFY
    {qualify_filters}
ORDER BY date, instrument
'''

SQL_JOIN_TEMPLATE = '''
WITH {table_id} AS (
{sql}
)
SELECT
    {base_table_id}.*
FROM {base_table_id}
JOIN {table_id} USING(date, instrument)
'''


def _build_filters(filters, options, selected, table, column, operator):
    if set(selected) == set(options):
        return

    filters.append({
        # split to remove USING etc.
        "column": column and f"{table.split(' ', 1)[0]}.{column}",
        "operator": operator,
        "value": [options[x] for x in selected],
        "table": table,
    })


def _value(v):
    if isinstance(v, str):
        return v.__repr__()
    return str(v)


def _build_sql_for_filters(filters):
    where_filters = []
    qualify_filters = []

    # TODO: where filter vs qualify filter
    for x in filters:
        if x["operator"] in {">", ">=", "=", "<=", "<"}:
            s = f'{x["column"]} {x["operator"]} {_value(x["value"])}'
        elif x["operator"] in {"OR"}:
            s = "(" + f' {x["operator"]} '.join(x["value"]) + ")" # f'{x["column"]} BETWEEN {_value(x["value"][0])} AND {_value(x["value"][1])}'
        elif x["operator"] in {"between"}:
            s = f'{x["column"]} BETWEEN {_value(x["value"][0])} AND {_value(x["value"][1])}'
        elif x["operator"] in {"rank_asc"}:
            # TODO: fix this, where or qualify, 怎么定义？
            s = f'c_rank({x["column"]}) <= {_value(x["value"])}'
        elif x["operator"] in {"rank_desc"}:
            # TODO: fix this, where or qualify, 怎么定义？
            s = f'c_rank(-1 * {x["column"]}) <= {_value(x["value"])}'
        elif x["operator"] in {"IN", "NOT IN"}:
            # TODO: fix this, where or qualify, 怎么定义？
            s = f'{x["column"]} {x["operator"]} {tuple(x["value"])}'
        else:
            raise Exception(f"unknown operator in {x}")

        if x["operator"] in {"rank_asc", "rank_desc"}:
            qualify_filters.append(s)
        else:
            where_filters.append(s)

    if where_filters:
        where_filters = "\n    AND ".join(where_filters)
    else:
        where_filters = "1 = 1"

    if qualify_filters:
        qualify_filters = "\n    AND ".join(qualify_filters)
    else:
        qualify_filters = "1 = 1"

    return where_filters, qualify_filters


def _build_tables(filters):
    tables = ["cn_stock_bar1d"] + [x["table"] for x in filters]
    seen_tables = set()
    tables_s = []
    for x in tables:
        if x != "cn_stock_bar1d" and " USING" not in x:
            x += " USING(date, instrument)"
        if x in seen_tables:
            continue
        tables_s.append(x)
        seen_tables.add(x)
    return "\n    JOIN ".join(tables_s)


def _build_sql(filters):
    where_filters, qualify_filters = _build_sql_for_filters(filters)
    tables = _build_tables(filters)

    sql = SQL_TEMPLATE.format(
        where_filters=where_filters,
        qualify_filters=qualify_filters,
        tables=tables,
    )

    return sql


def _build_table(ds) -> dict:
    if isinstance(ds, str):
        sql = ds
    else:
        type_ = ds.type
        if type_ == "json":
            sql = ds.read()["sql"]
        elif type == "text":
            sql = ds.read()
        else:
            # bdb
            return {"sql": "", "table_id": ds.id}

    import bigdb

    table_id = f"_t_{uuid.uuid4().hex}"
    parts = [x.strip().strip(";") for x in bigdb.connect().parse_query(sql)]
    parts[-1] = f"CREATE TABLE {table_id} AS {parts[-1]}"
    sql = ";\n".join(parts)
    if sql:
        sql += ";\n"

    return {
        "sql": sql,
        "table_id": table_id,
    }


def _build_join_sql(base_query, sql):
    base_table = _build_table(base_query)
    table_id = f"_t_{uuid.uuid4().hex}"
    return base_table["sql"] + SQL_JOIN_TEMPLATE.format(
        base_table_id=base_table["table_id"],
        table_id=table_id,
        sql=sql
    )


def run(
    base_query: I.port("基础查询", specific_type_name="DataSource", optional=True) = None,
    exchanges: I.choice("交易所", list(EXCHANGES.keys()), multi=True) = list(EXCHANGES.keys()),
    list_sectors: I.choice("上市板块", list(LIST_SECTORS.keys()), multi=True) = list(LIST_SECTORS.keys()),
    indexes: I.choice("指数成分", list(INDEXES.keys()), multi=True) = list(INDEXES.keys()),
    st_statuses: I.choice("ST状态", list(ST_STATUSES.keys()), multi=True) = list(ST_STATUSES.keys()),
    margin_tradings: I.choice("融资融券", list(MARGIN_TRADINGS.keys()), multi=True) = list(MARGIN_TRADINGS.keys()),
    sw2021_industries: I.choice("行业/申万2021", list(SW2021_INDUSTRIES.keys()), multi=True) = list(SW2021_INDUSTRIES.keys()),
    drop_suspended: I.bool("过滤停牌") = False,
) -> [
    I.port("查询", "data")
]:
    import dai

    filters = []
    _build_filters(filters, EXCHANGES, exchanges, "cn_stock_bar1d", None, "OR")
    _build_filters(filters, LIST_SECTORS, list_sectors, "cn_stock_basic_info USING (instrument)", "list_sector", "IN")
    _build_filters(filters, INDEXES, indexes, "cn_stock_factors_base", None, "OR")
    _build_filters(filters, ST_STATUSES, st_statuses, "cn_stock_status", "st_status", "IN")
    _build_filters(filters, MARGIN_TRADINGS, margin_tradings, "cn_stock_factors_base", "margin_trading_status", "IN")
    _build_filters(filters, SW2021_INDUSTRIES, sw2021_industries, "cn_stock_factors_base", "sw2021_level1", "IN")
    if drop_suspended:
        filters.append({
            # split to remove USING etc.
            "column": "cn_stock_factors_base.suspended",
            "operator": "=",
            "value": 0,
            "table": "cn_stock_factors_base",
        })

    sql = _build_sql(filters)
    if base_query is not None:
        sql = _build_join_sql(base_query, sql)

    return I.Outputs(data=dai.DataSource.write_json({"sql": sql}))


def post_run(outputs):
    """后置运行函数"""
    return outputs
