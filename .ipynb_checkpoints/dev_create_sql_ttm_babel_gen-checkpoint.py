import pandas as pd
import json
import argparse
import sys
from collections import defaultdict
from datetime import datetime

# コマンドライン引数の設定
parser = argparse.ArgumentParser(description="Generate Snowflake SELECT queries from CSV LOG data.")
parser.add_argument("csv_file", help="Path to the CSV file containing LOG data.")
args = parser.parse_args()

# CSVデータを読み込み
df = pd.read_csv(args.csv_file)
table_name_df = pd.read_csv("./source_data/babel_table_name.csv")

# IDをキーにした辞書を作成します
# ここでは、IDごとに他のカラムの情報を辞書にまとめています
data_map = table_name_df.set_index('LOG_TYPE').to_dict(orient='index')
# デフォルトの変数名
default_label = "default_table"
integration = "dblog_dev_int"
source_table_name = "TTM_BABEL.BABEL_STG_DEV.DBLOG"
task_schedule = '60 MINUTE'

from datetime import datetime, date


# Pythonの型からSnowflakeの型へのマッピング辞書
type_mapping = {
    str: "STRING",
    int: "NUMBER",
    float: "FLOAT",
    bool: "BOOLEAN",
    dict: "VARIANT",      # ディクショナリーはVARIANTとする
    date: "DATE",         # Pythonのdate型に対応
    datetime: "TIMESTAMP" # Pythonのdatetime型に対応
}
# 逆引きマップ
inverse_type_mapping = {value: key for key, value in type_mapping.items()}

# 文字列が日付や日時の形式かどうかを判定する関数
def detect_date_type(value):
    try:
        # ISO形式の日付として解析可能ならDATE型
        date.fromisoformat(value)
        return "DATE"
    except ValueError:
        pass
    try:
        # ISO形式の日時として解析可能ならTIMESTAMP型
        datetime.fromisoformat(value)
        return "TIMESTAMP"
    except ValueError:
        pass
    return "STRING"  # どちらでもなければSTRINGと判断

# Pythonの型からSnowflakeの型を取得する関数
def get_snowflake_type(value):
    if value is None:
        return "__EMPTY__"
    # リストの場合、中の型も考慮してARRAY<型>形式にする
    if isinstance(value, list):
        # 空のリストの場合はARRAY<__EMPTY__>とする
        if not value:
            return "ARRAY<__EMPTY__>"
        # 最初の要素の型でARRAYの要素型を決定
        element_type = get_snowflake_type(value[0])
        return f"ARRAY<{element_type}>"
    
    # 辞書の場合はVARIANTとして扱う
    # inverse_type_mappingに存在しないタイプはVARIANTとして扱うため、元のタイプが推測できるよう__DICT__を設定する
    elif isinstance(value, dict):
        return "__DICT__"
    
    # 文字列の場合、日付や日時の形式かを判定
    elif isinstance(value, str):
        return detect_date_type(value)
    
    # その他の型はマッピング辞書で取得
    else:
        return type_mapping.get(type(value), "__Unknown__")

def update_dict(d, key, new_snowflake_type):
    """
    log_dict[key] の型情報を new_snowflake_type と照合し、必要なら更新する。
    更新前後の型情報が異なる場合は、標準エラー出力にログ出力する。
    """
    if key not in d:
        d[key] = new_snowflake_type
        return
    if d[key] == new_snowflake_type:
        return
    if d[key] == "__EMPTY__":
        d[key] = new_snowflake_type
    elif d[key] == "ARRAY<__EMPTY__>" and new_snowflake_type.endswith("__EMPTY__"):
        d[key] = new_snowflake_type

    if "__EMPTY__" not in d[key] and "__EMPTY__" not in new_snowflake_type:
         ValueError(f"無効な値です: key={key}, before={d[key]} after={new_snowflake_type}")

def flatten_dict(d, parent_key='', sep='.'):
    """辞書を再帰的にフラット化し、'親キー.子キー' の形式の辞書を返す。"""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        # elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
        #     # 辞書の配列の場合、最初の1個目の要素でカラムを決定する
        #     items.update(flatten_dict(v[0], new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def flatten_update_dict(keys, key, log_value, parent_snowflake_type=''):
    snowflake_type = get_snowflake_type(log_value)
    # snowflake_typeが辞書の場合は再帰的にフラット化して各キーに対して既存ロジックを適用する
    if snowflake_type == '__DICT__':
        flat_log_value = flatten_dict(log_value, key)
        for new_key, new_value in flat_log_value.items():
            flatten_update_dict(keys, new_key, new_value, parent_snowflake_type)
    elif snowflake_type == 'ARRAY<__DICT__>' and len(log_value) > 0:
        # 辞書の配列の場合、最初の1個目の要素でカラムを決定する
        flat_log_value = flatten_dict(log_value[0], key)
        for new_key, new_value in flat_log_value.items():
            flatten_update_dict(keys, new_key, new_value, f'{parent_snowflake_type}{snowflake_type}.')
    else:
        try:
            update_dict(keys, key, f'{parent_snowflake_type}{snowflake_type}')
        except Exception as e:
            print(f"flatten_update_dict error: {e}", file=sys.stderr)

def get_group_keys(df):
    # `LOG_TYPE`ごとにログのJSONキーを集計
    group_keys = defaultdict(dict)
    for _, row in df.iterrows():
        if not pd.notna(row['LOG_TYPE']):
            continue
        LOG_TYPE = row['LOG_TYPE']
        LOG_data = json.loads(row['LOG'])
        # エスケープされている場合もう一度パースする
        if isinstance(LOG_data, str):
            LOG_data = json.loads(LOG_data)

        keys = group_keys[LOG_TYPE]
        for key in LOG_data.keys():
            log_value = LOG_data.get(key, None)
            flatten_update_dict(keys, key, log_value)

        group_keys[LOG_TYPE] = keys
    return group_keys

def get_array_field(key, key_type, sep='_'):
    keys = key.split('.')
    if len(keys) > 1:
        child_key_type = key_type.split('.')[-1]      
        if child_key_type.startswith("ARRAY"):
            return sep.join(keys)
        return sep.join(keys[0:-1])
    return key

def gen_create_queries(group_keys):
    # `LOG_TYPE`ごとのSELECTクエリを生成
    queries = []

    for LOG_TYPE in sorted(group_keys.keys()):
        try:
            table_name = data_map[LOG_TYPE]['TABLE_NAME']
            keys = group_keys[LOG_TYPE]

            array_keys = defaultdict(dict)
            for key, key_type in keys.items():
                if "ARRAY" in key_type:
                    array_field_accessor = get_array_field(key, key_type, '.')
                    array_keys[array_field_accessor] = key_type
            has_single_array = len(array_keys) == 1
            multi_array_fields = defaultdict(dict)

            insert_fields = []
            select_fields = []
            for key, key_type in keys.items():
                field = key.replace(".", "_")
                # insert_fields, select_fieldsのセット
                if key_type != "VARIANT" and key_type in inverse_type_mapping:
                    # key_typeが拡張していないsnowflakeに存在する型の場合castする
                    insert_fields.append(f'{field} {key_type} NULL')
                    select_fields.append(f'CAST(PARSE_JSON(LOG):"{key}" AS {key_type}) AS {field}')
                elif "ARRAY" in key_type and has_single_array:
                    # 配列が1つのときのみ対応する
                    array_field = get_array_field(key, key_type)
                    child_field = key.split('.')[-1]
                    child_key_type = key_type.split('.')[-1]
                    if "ARRAY" in child_key_type:
                        child_key_type = child_key_type[child_key_type.find('<')+1:child_key_type.find('>')]
                    insert_fields.append(f'{field} {child_key_type} NULL')
                    select_fields.append(f'{array_field}.value:"{child_field}"::{child_key_type} AS {field}')
                elif "ARRAY" in key_type and not has_single_array:
                    array_field = get_array_field(key, key_type)
                    if array_field in multi_array_fields:
                        continue
                    multi_array_fields[array_field] = array_field
                    insert_fields.append(f'{array_field} VARIANT NULL')
                    select_fields.append(f'PARSE_JSON(LOG):"{array_field}" AS {array_field}')
                else:
                    # その他の型の場合はそのままVARIANTとする
                    insert_fields.append(f'{field} VARIANT  NULL')
                    select_fields.append(f'PARSE_JSON(LOG):"{key}" AS {field}')
                    select_fields.append(f'-- {key}: {key_type} --')                

            table_query = [source_table_name]
            # 配列用に特殊処理
            array_create_index_str = ""
            array_select_index_str = ""
            # 配列が1つのときのみ対応する
            if has_single_array:
                for array_key, array_key_type in array_keys.items():
                    array_field = '_'.join(array_key.split('.'))
                    array_field_accessor = array_key
                    if array_select_index_str == "":
                        array_create_index_str = "idx INT NOT NULL,"
                        array_select_index_str = f'{array_field}.index AS idx,'
                    table_query.append(f'LATERAL FLATTEN(input => PARSE_JSON(LOG):"{array_field_accessor}") {array_field}')

            # 各種fieldsを改行区切りで結合
            insert_fields_str = ",\n    ".join(insert_fields)
            select_fields_str = ",\n    ".join(select_fields)
            table_query_str = ",\n    ".join(table_query)

            query = f"""
CREATE OR REPLACE TABLE {table_name}(
    id INT NOT NULL,
    {array_create_index_str}
    request_id STRING NULL,
    created TIMESTAMP NULL,
    LOGs_userId STRING NULL,
    {insert_fields_str}
) AS
SELECT DISTINCT
    id,
    {array_select_index_str}
    request_id,
    created,
    user_id as LOGs_userId,
    {select_fields_str}
FROM {table_query_str}
WHERE LOG_TYPE = '{LOG_TYPE}'
;
"""
            queries.append((LOG_TYPE, query))
        except KeyError:
            # LOG_TYPEが見つからない場合はスキップ
            continue
    return queries

def gen_task_queries(group_keys):
    # `LOG_TYPE`ごとのSELECTクエリを生成
    queries = []
    for LOG_TYPE in sorted(group_keys.keys()):
        try:
            table_name = data_map[LOG_TYPE]['TABLE_NAME']
            keys = group_keys[LOG_TYPE]

            array_keys = defaultdict(dict)
            for key, key_type in keys.items():
                if "ARRAY" in key_type:
                    array_field_accessor = get_array_field(key, key_type, '.')
                    array_keys[array_field_accessor] = key_type
            has_single_array = len(array_keys) == 1
            multi_array_fields = defaultdict(dict)

            select_fields = []
            for key, key_type in keys.items():
                field = key.replace(".", "_")
                # insert_fields, select_fieldsのセット
                if key_type != "VARIANT" and key_type in inverse_type_mapping:
                    # key_typeが拡張していないsnowflakeに存在する型の場合castする
                    select_fields.append(f'CAST(PARSE_JSON(LOG):"{key}" AS {key_type}) AS {field}')
                elif "ARRAY" in key_type and has_single_array:
                    # 配列が1つのときのみ対応する
                    array_field = get_array_field(key, key_type)
                    child_field = key.split('.')[-1]
                    child_key_type = key_type.split('.')[-1]
                    if "ARRAY" in child_key_type:
                        child_key_type = child_key_type[child_key_type.find('<')+1:child_key_type.find('>')]
                    select_fields.append(f'{array_field}.value:"{child_field}"::{child_key_type} AS {field}')
                elif "ARRAY" in key_type and not has_single_array:
                    array_field = get_array_field(key, key_type)
                    if array_field in multi_array_fields:
                        continue
                    multi_array_fields[array_field] = array_field
                    select_fields.append(f'PARSE_JSON(LOG):"{array_field}" AS {array_field}')
                else:
                    # その他の型の場合はそのまま
                    select_fields.append(f'PARSE_JSON(LOG):"{key}" AS {field}')
                    select_fields.append(f'-- {key}: {key_type} --')

            stream_name = table_name + '_STERAM'
            table_query = [stream_name]
            # 配列用に特殊処理
            array_select_index_str = ""
            array_join_idx_str = ""
            array_insert_idx1_str = ""
            array_insert_idx2_str = ""
            # 配列が1つのときのみ対応する
            if has_single_array:
                array_join_idx_str = "AND tgt.idx = src.idx"
                array_insert_idx1_str = "idx,"
                array_insert_idx2_str = "src.idx,"
                for array_key, array_key_type in array_keys.items():
                    array_field = '_'.join(array_key.split('.'))
                    array_field_accessor = array_key
                    if array_select_index_str == "":
                        array_select_index_str = f'{array_field}.index AS idx,'
                    table_query.append(f'LATERAL FLATTEN(input => PARSE_JSON(LOG):"{array_field_accessor}") {array_field}')

            update_fields = []
            insert_fields1 = []
            insert_fields2 = []
            multi_array_fields = defaultdict(dict)
            for key, key_type in keys.items():
                field = key.replace(".", "_")
                if "ARRAY" in key_type and not has_single_array:
                    array_field = get_array_field(key, key_type)
                    if array_field in multi_array_fields:
                        continue
                    multi_array_fields[array_field] = array_field
                    field = array_field
                update_fields.append(f'tgt.{field} = src.{field}')
                insert_fields1.append(f'{field}')
                insert_fields2.append(f'src.{field}')

            # 各種fieldsを改行区切りで結合
            select_fields_str = ",\n        ".join(select_fields)
            update_fields_str = ",\n    ".join(update_fields)
            insert_fields1_str = ",\n    ".join(insert_fields1)
            insert_fields2_str = ",\n    ".join(insert_fields2)
            table_query_str = ",\n        ".join(table_query)

            query = f"""
CREATE OR REPLACE STREAM {stream_name} on TABLE DBLOG;

CREATE OR REPLACE TASK {table_name}_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = {integration}
SCHEDULE = '{task_schedule}'
AS 
MERGE INTO {table_name} AS tgt
USING (
    SELECT DISTINCT 
        id,
        {array_select_index_str}
        request_id,
        created,
        user_id AS LOGs_userId,
        {select_fields_str},
        metadata$action
    FROM {table_query_str}
    WHERE LOG_TYPE = '{LOG_TYPE}'
) src
ON tgt.id = src.id
   {array_join_idx_str}
WHEN MATCHED AND src.metadata$action = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    tgt.request_id = src.request_id,
    tgt.created = src.created,
    tgt.LOGs_userId = src.LOGs_userId,
    {update_fields_str}
WHEN NOT MATCHED THEN INSERT (
    id,
    {array_insert_idx1_str}
    request_id,
    created,
    LOGs_userId,
    {insert_fields1_str}
) VALUES (
    src.id,
    {array_insert_idx2_str}
    src.request_id,
    src.created,
    src.LOGs_userId,
    {insert_fields2_str}
);
--Task再開
ALTER TASK {table_name}_TASK RESUME;

--Task実行
EXECUTE TASK {table_name}_TASK;
"""
            queries.append((LOG_TYPE, query))
        except KeyError:
            # LOG_TYPEが見つからない場合はスキップ
            continue
    return queries

group_keys = get_group_keys(df)
suffix = datetime.now().strftime("%Y%m%d%H%M%S")

# クエリ結果を出力\
# テーブル作成クエリ
with open(f'./stg_create_tbl_sql/dev_staging_table.sql', "a") as file:
    file.write(
"""
{% if env == "DEV" %}
-- DEVの場合の処理
　USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% elif env == "PROD" %}
-- PRODの場合の処理
　USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
    {{ raise_error("DEVかPRODを指定してください") }}
{% endif %}
"""
    )
    queries = gen_create_queries(group_keys)
    for LOG_TYPE, query in queries:
        # print(f"-- Query for LOG_TYPE = {LOG_TYPE}\n{query}\n")
        file.write(f"-- Query for LOG_TYPE = {LOG_TYPE}\n{query}\n")

# タスククエリ
with open(f'./stg_create_task_sql/dev_task.sql', "a") as file:
    queries = gen_task_queries(group_keys)
    for LOG_TYPE, query in queries:
        # print(f"-- Query for LOG_TYPE = {LOG_TYPE}\n{query}\n")
        file.write(f"-- Query for LOG_TYPE = {LOG_TYPE}\n{query}\n")
