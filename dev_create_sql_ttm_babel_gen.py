import pandas as pd
import json
import argparse
import sys
from collections import defaultdict
from datetime import datetime, date


class SnowflakeSQLGenerator:
    def __init__(self, log_csv_path, table_csv_path="./source_data/babel_table_name.csv",
                 integration="dblog_dev_int", source_table_name="DBLOG", task_schedule="24 HOUR"):
        self.df = pd.read_csv(log_csv_path)
        self.table_name_df = pd.read_csv(table_csv_path)
        self.integration = integration
        self.source_table_name = source_table_name
        self.task_schedule = task_schedule

        # Build mapping from LOG_TYPE to additional table info
        self.data_map = self.table_name_df.set_index('LOG_TYPE').to_dict(orient='index')

        # Define type mapping for Snowflake
        self.type_mapping = {
            str: "STRING",
            int: "NUMBER",
            float: "FLOAT",
            bool: "BOOLEAN",
            dict: "VARIANT",   # dictionaries are treated as VARIANT
            date: "DATE",
            datetime: "TIMESTAMP"
        }
        self.inverse_type_mapping = {value: key for key, value in self.type_mapping.items()}

    # --- Type Detection and Conversion Utilities ---

    def detect_date_type(self, value):
        """Return DATE/TIMESTAMP if value is in ISO format, else STRING."""
        try:
            date.fromisoformat(value)
            return "DATE"
        except ValueError:
            pass
        try:
            datetime.fromisoformat(value)
            return "TIMESTAMP"
        except ValueError:
            pass
        return "STRING"

    def get_snowflake_type(self, value):
        """Determine the Snowflake type for a given Python value."""
        if value is None:
            return "__EMPTY__"
        if isinstance(value, list):
            if not value:
                return "ARRAY<__EMPTY__>"
            element_type = self.get_snowflake_type(value[0])
            return f"ARRAY<{element_type}>"
        elif isinstance(value, dict):
            return "__DICT__"
        elif isinstance(value, str):
            return self.detect_date_type(value)
        else:
            return self.type_mapping.get(type(value), "__Unknown__")

    def update_dict(self, d, key, new_snowflake_type):
        """
        Update the dictionary for key with new_snowflake_type.
        If existing type is __EMPTY__ it is replaced;
        otherwise, a conflict (without __EMPTY__) raises an error.
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
        else:
            if "__EMPTY__" not in d[key] and "__EMPTY__" not in new_snowflake_type:
                raise ValueError(f"Invalid type update: key={key}, before={d[key]}, after={new_snowflake_type}")

    def flatten_dict(self, d, parent_key='', sep='.'):
        """
        Recursively flattens a nested dictionary into a single-level dictionary
        with keys in 'parent.child' format.
        """
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self.flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    def flatten_update_dict(self, keys, key, log_value, parent_snowflake_type=''):
        """
        Update keys with flattened values.
        If log_value is a dictionary, it is flattened and processed recursively.
        """
        snowflake_type = self.get_snowflake_type(log_value)
        if snowflake_type == '__DICT__':
            flat_log_value = self.flatten_dict(log_value, key)
            for new_key, new_value in flat_log_value.items():
                self.flatten_update_dict(keys, new_key, new_value, parent_snowflake_type)
        elif snowflake_type == 'ARRAY<__DICT__>' and len(log_value) > 0:
            for v in log_value:
                flat_log_value = self.flatten_dict(v, key)
                for new_key, new_value in flat_log_value.items():
                    self.flatten_update_dict(keys, new_key, new_value,
                                             f'{parent_snowflake_type}{snowflake_type}.')
        else:
            try:
                self.update_dict(keys, key, f'{parent_snowflake_type}{snowflake_type}')
            except Exception as e:
                print(f"flatten_update_dict error: {e}", file=sys.stderr)

    def get_group_keys(self):
        """
        Build a dictionary of keys (and their types) for each LOG_TYPE found in the CSV log data.
        """
        group_keys = defaultdict(dict)
        for _, row in self.df.iterrows():
            if not pd.notna(row['LOG_TYPE']):
                continue
            log_type = row['LOG_TYPE']
            log_data = json.loads(row['LOG'])
            # In case the JSON is double-encoded
            if isinstance(log_data, str):
                log_data = json.loads(log_data)

            keys = group_keys[log_type]
            for key in log_data.keys():
                log_value = log_data.get(key, None)
                self.flatten_update_dict(keys, key, log_value)
            group_keys[log_type] = keys
        return group_keys

    def get_array_field(self, key, key_type, sep='_'):
        """
        Return the appropriate array field name based on key structure and key_type.
        """
        keys = key.split('.')
        if len(keys) > 1:
            child_key_type = key_type.split('.')[-1]
            if child_key_type.startswith("ARRAY"):
                return sep.join(keys)
            return sep.join(keys[:-1])
        return key

    # --- Query Generation Methods ---

    def gen_create_queries(self, group_keys):
        """
        Generate CREATE TABLE queries for each LOG_TYPE.
        """
        queries = []
        for log_type in sorted(group_keys.keys()):
            try:
                table_name = self.data_map[log_type]['TABLE_NAME']
                keys = group_keys[log_type]

                # Identify array fields
                array_keys = defaultdict(dict)
                for key, key_type in keys.items():
                    if "ARRAY" in key_type:
                        array_field_accessor = self.get_array_field(key, key_type, '.')
                        array_keys[array_field_accessor] = key_type
                has_single_array = len(array_keys) == 1
                multi_array_fields = defaultdict(dict)

                insert_fields = []
                select_fields = []
                for key, key_type in keys.items():
                    field = key.replace(".", "_")
                    if key_type != "VARIANT" and key_type in self.inverse_type_mapping:
                        insert_fields.append(f'{field} {key_type} NULL')
                        select_fields.append(f'CAST(PARSE_JSON(LOG):{key} AS {key_type}) AS {field}')
                    elif "ARRAY" in key_type and has_single_array:
                        array_field = self.get_array_field(key, key_type)
                        child_field = key.split('.')[-1]
                        child_key_type = key_type.split('.')[-1]
                        if "ARRAY" in child_key_type:
                            child_key_type = child_key_type[child_key_type.find("<") + 1:child_key_type.find(">")]
                        if child_key_type in self.inverse_type_mapping:
                            insert_fields.append(f'{field} {child_key_type} NULL')
                            select_fields.append(f'{array_field}.value:{child_field}::{child_key_type} AS {field}')
                        else:
                            insert_fields.append(f'{field} VARIANT NULL')
                            select_fields.append(f'{array_field}.value:{child_field} AS {field}')
                    elif "ARRAY" in key_type and not has_single_array:
                        array_field = self.get_array_field(key, key_type)
                        array_key = self.get_array_field(key, key_type, '.')
                        if array_field in multi_array_fields:
                            continue
                        multi_array_fields[array_field] = array_field
                        insert_fields.append(f'{array_field} VARIANT NULL')
                        select_fields.append(f'PARSE_JSON(LOG):{array_key} AS {array_field}')
                    else:
                        insert_fields.append(f'{field} VARIANT NULL')
                        select_fields.append(f'PARSE_JSON(LOG):{key} AS {field}')
                        select_fields.append(f'-- {key}: {key_type} --')

                table_query = [self.source_table_name]
                array_create_index_str = ""
                array_select_index_str = ""
                if has_single_array:
                    for array_key, _ in array_keys.items():
                        array_field = '_'.join(array_key.split('.'))
                        if not array_select_index_str:
                            array_create_index_str = "idx INT NOT NULL,"
                            array_select_index_str = f'{array_field}.index AS idx,'
                        table_query.append(f'LATERAL FLATTEN(input => PARSE_JSON(LOG):"{array_key}") {array_field}')

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
WHERE LOG_TYPE = '{log_type}'
;
"""
                queries.append((log_type, query))
            except KeyError:
                # Skip if LOG_TYPE mapping is missing
                continue
        return queries

    def gen_task_queries(self, group_keys):
        """
        Generate TASK queries (with MERGE statements) for each LOG_TYPE.
        """
        queries = []
        for log_type in sorted(group_keys.keys()):
            try:
                table_name = self.data_map[log_type]['TABLE_NAME']
                keys = group_keys[log_type]

                array_keys = defaultdict(dict)
                for key, key_type in keys.items():
                    if "ARRAY" in key_type:
                        array_field_accessor = self.get_array_field(key, key_type, '.')
                        array_keys[array_field_accessor] = key_type
                has_single_array = len(array_keys) == 1
                multi_array_fields = defaultdict(dict)
                select_fields = []
                for key, key_type in keys.items():
                    field = key.replace(".", "_")
                    if key_type != "VARIANT" and key_type in self.inverse_type_mapping:
                        select_fields.append(f'CAST(PARSE_JSON(LOG):{key} AS {key_type}) AS {field}')
                    elif "ARRAY" in key_type and has_single_array:
                        array_field = self.get_array_field(key, key_type)
                        child_field = key.split('.')[-1]
                        child_key_type = key_type.split('.')[-1]
                        if "ARRAY" in child_key_type:
                            child_key_type = child_key_type[child_key_type.find("<") + 1:child_key_type.find(">")]
                        if child_key_type in self.inverse_type_mapping:
                            select_fields.append(f'{array_field}.value:{child_field}::{child_key_type} AS {field}')
                        else:
                            select_fields.append(f'{array_field}.value:{child_field} AS {field}')
                    elif "ARRAY" in key_type and not has_single_array:
                        array_field = self.get_array_field(key, key_type)
                        array_key = self.get_array_field(key, key_type, '.')
                        if array_field in multi_array_fields:
                            continue
                        multi_array_fields[array_field] = array_field
                        select_fields.append(f'PARSE_JSON(LOG):{array_key} AS {array_field}')
                    else:
                        select_fields.append(f'PARSE_JSON(LOG):{key} AS {field}')
                        select_fields.append(f'-- {key}: {key_type} --')

                stream_name = table_name + '_STERAM'
                table_query = [stream_name]
                array_select_index_str = ""
                array_join_idx_str = ""
                array_insert_idx1_str = ""
                array_insert_idx2_str = ""
                if has_single_array:
                    array_join_idx_str = "AND tgt.idx = src.idx"
                    array_insert_idx1_str = "idx,"
                    array_insert_idx2_str = "src.idx,"
                    for array_key, _ in array_keys.items():
                        array_field = '_'.join(array_key.split('.'))
                        if not array_select_index_str:
                            array_select_index_str = f'{array_field}.index AS idx,'
                        table_query.append(f'LATERAL FLATTEN(input => PARSE_JSON(LOG):"{array_key}") {array_field}')

                update_fields = []
                insert_fields1 = []
                insert_fields2 = []
                multi_array_fields = defaultdict(dict)
                for key, key_type in keys.items():
                    field = key.replace(".", "_")
                    if "ARRAY" in key_type and not has_single_array:
                        array_field = self.get_array_field(key, key_type)
                        if array_field in multi_array_fields:
                            continue
                        multi_array_fields[array_field] = array_field
                        field = array_field
                    update_fields.append(f'tgt.{field} = src.{field}')
                    insert_fields1.append(f'{field}')
                    insert_fields2.append(f'src.{field}')

                select_fields_str = ",\n        ".join(select_fields)
                update_fields_str = ",\n    ".join(update_fields)
                insert_fields1_str = ",\n    ".join(insert_fields1)
                insert_fields2_str = ",\n    ".join(insert_fields2)
                table_query_str = ",\n        ".join(table_query)

                query = f"""
CREATE OR REPLACE STREAM {stream_name} on TABLE DBLOG;

CREATE OR REPLACE TASK {table_name}_TASK
WAREHOUSE = TTM_BABEL_XS
ERROR_INTEGRATION = {self.integration}
SCHEDULE = '{self.task_schedule}'
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
    WHERE LOG_TYPE = '{log_type}'
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
                queries.append((log_type, query))
            except KeyError:
                continue
        return queries

    # --- File Writing Helper ---

    def write_queries_to_file(self, queries, file_prefix, chunk_count, schema_template):
        def split_by_chunks(data, n):
            k, m = divmod(len(data), n)
            return [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]
        split_queries = split_by_chunks(queries, chunk_count)
        for i, queries_chunk in enumerate(split_queries):
            filename = f'./{file_prefix}{i + 1}.sql'
            with open(filename, "w") as file:
                file.write(schema_template)
                for log_type, query in queries_chunk:
                    file.write(f"-- Query for LOG_TYPE = {log_type}\n{query}\n")

    # --- Main Generation Method ---

    def generate(self):
        group_keys = self.get_group_keys()
        schema_template = """
{% if env == "PROD" %}
-- PRODの場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_PROD;
{% else %}
-- その他の場合の処理
USE SCHEMA TTM_BABEL.BABEL_STG_DEV;
{% endif %}
"""
        create_queries = self.gen_create_queries(group_keys)
        task_queries = self.gen_task_queries(group_keys)
        self.write_queries_to_file(create_queries, 'dev_staging_table', 1, schema_template)
        self.write_queries_to_file(task_queries, 'dev_task', 2, schema_template)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Snowflake SELECT queries from CSV LOG data.")
    parser.add_argument("csv_file", help="Path to the CSV file containing LOG data.")
    parser.add_argument("--table_csv", default="./source_data/babel_table_name.csv",
                        help="Path to the CSV file containing table mapping data.")
    return parser.parse_args()


def main():
    args = parse_args()
    generator = SnowflakeSQLGenerator(args.csv_file, args.table_csv)
    generator.generate()


if __name__ == "__main__":
    main()
