import pandas as pd
import numpy as np
import os
import sqlparse
from sqlparse.sql import Parenthesis
import time

pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 5000)


def select_by_schema(table_name, db_name):
    """
    index optimization
    :param table_name:
    :param db_name:
    :return:
    """
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    df.set_index('table_schema', drop=False, inplace=True)
    try:
        data = df.loc[db_name]
        return data
    except KeyError:
        return None


def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def insert(table_name, data, unique_columns=None, unique_values=None):
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if unique_columns:
        temp_df = df.copy()
        for key, value in zip(unique_columns, unique_values):
            temp_df = temp_df[temp_df[key] == value]
        idx = temp_df.index
        if len(idx) > 0:
            return False

    df.loc[len(df)] = data
    df.to_csv(f'./data/{table_name}.csv', index=False)
    return True


def select(table_name, raw_attributes, where_key=None, where_value=None):
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if table_name == 'schema':
        df.set_index('db_name', inplace=True, drop=False)
    if table_name == 'columns':
        df.set_index(['table_schema', 'table_name'], inplace=True, drop=False)
    if table_name == 'index':
        df.set_index(['table_schema', 'table_name', 'index_name'], inplace=True, drop=False)
    if table_name == 'columns':
        df.set_index(['table_schema', 'table_name', 'table_name'], inplace=True, drop=False)
    df = df.applymap(lambda x: str(x))
    attributes = df.columns if raw_attributes == ['*'] else raw_attributes
    if where_key:
        for key, value in zip(where_key, where_value):
            df = df[df[key].str.lower() == value]
        return df[attributes]
    else:
        return df[attributes]


def delete(table_name, key=None, value=None):
    if key:
        df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
        if isinstance(key, str):
            idx = df.loc[(df[key] == value)].index
            if len(idx) == 0:
                return False
            df = df.drop(index=idx)
        else:
            exist = False
            df_len = len(df)
            for i in range(df_len):
                skip = False
                for k, v in zip(key, value):
                    if df.loc[i, k] != v:
                        skip = True
                        break
                if not skip:
                    df = df.drop(index=i)
                    exist = True
            if not exist:
                return False
        df.to_csv(f'./data/{table_name}.csv', index=False)
    else:
        os.remove(f"./data/{table_name}.csv")
    return True


def update(table_name, column_name, column_value, where_key, where_value):
    df = pd.read_csv(f'./data/{table_name}.csv', encoding='utf8')
    if isinstance(where_key, str):
        idx = df.loc[df[where_key] == where_value].index
    else:
        tmp_df = df.copy()
        for k, v in zip(where_key, where_value):
            tmp_df = tmp_df[tmp_df[k] == v]
        idx = tmp_df.index
    if len(idx) == 0:
        return False
    df.loc[idx, column_name] = column_value
    df.to_csv(f'./data/{table_name}.csv', index=False)
    return True


def proc_create_db(tokens):
    """
    CREATE DATABASE <数据库名>
    [CHARACTER SET <字符集名>]
    [COLLATE <校对规则名>];
    :param tokens
    :return:
    """

    db_name = tokens[2]
    try:
        charset_name = tokens.index('set')
    except ValueError:
        charset_name = 'utf8'
    try:
        collate_name = tokens.index('collate')
    except ValueError:
        collate_name = 'utf8_general_ci'

    # data = ['db_name', 'charset', 'collate']
    data = [db_name, charset_name, collate_name]
    ret = insert('schema', data, unique_columns=['db_name'], unique_values=[db_name])
    if not ret:
        error(f"db [{db_name}]] already exists!")
    else:
        print("[INFO] create database successfully")


def error(info=None):
    if info:
        print('[ERROR]', info)


def in_list(key, df):
    for item in df.values.tolist():
        if key == item:
            return True
    return False


def proc_create_table(db_name, tokens):
    """
    CREATE TABLE <表名> ([表定义选项])[表选项][分区选项];
    [表定义选项]: <列名1> <类型1> [,…] <列名n> <类型n>
    :param tokens:
    :return:
    """
    # print(tokens)
    table_name = tokens[2]
    item_list = []
    stack = []
    i = tokens.index('(')
    stack.append('(')
    column_pos = 0
    last_k = len(tokens) - 2 - tokens[::-1].index(')')
    key_name = ''
    key_type = ''
    auto_increment_idx = ''
    # insert into tables
    while len(stack) > 0:
        i += 1
        if tokens[i] == ')':
            if stack[-1] == '(':
                stack.pop()
            else:
                error("[ERROR] invalid sql")
                break
        else:
            column_pos += 1

            if tokens[i] in ['primary']:
                key_name = tokens[tokens.index('primary') + 3]
                key_type = 'primary'
                while tokens[i] != ',' and i < last_k:
                    i += 1
            else:
                default_value = ''
                is_nullable = True
                key_len = ''
                auto_increment = False
                column_name = tokens[i]
                i += 1
                key_type = tokens[i]
                i += 1
                if tokens[i] == '(':
                    key_len = tokens[i + 1]
                    i += 3
                if tokens[i] == 'not' and tokens[i + 1] == 'null':
                    is_nullable = False
                    i += 2
                if 'auto_increment' in tokens[i:]:
                    auto_increment = True
                    idx = tokens.index('auto_increment')
                    if tokens[idx + 1] == '=':
                        auto_increment_idx = tokens[idx + 2]
                if 'default' in tokens[i:]:
                    default_value = tokens[tokens.index('default') + 1]
                if auto_increment and auto_increment_idx == '':
                    auto_increment_idx = 0
                item_list.append(
                    [column_name, column_pos, default_value, is_nullable, key_type, key_len, auto_increment])
                while tokens[i] != ',' and i < last_k:
                    i += 1

                # print(item_list[-1])
    # db_name, table_name, column_name, column_pos, default_value, is_nullable, key_type, key_len, auto_increment,
    # column_key
    head_data = [db_name, table_name]
    df = select('columns', raw_attributes=['table_schema', 'table_name'])

    if in_list(head_data, df):
        error(f"db [{db_name}] table [{table_name}] already exists!")
        return

    # get engin
    try:
        engine = tokens[tokens.index('engine') + 2]
    except ValueError:
        engine = 'innodb'
    # insert into tables.csv
    try:
        charset = tokens[tokens.index('charset') + 2]
    except ValueError:
        print("charset=", charset)
        charset = 'utf8'
    try:
        table_type = tokens[tokens.index('table_type') + 2]
    except ValueError:
        table_type = 'base_table'

    insert('tables',
           [db_name, table_name, table_type, engine, 0, get_now_time(), auto_increment_idx, get_now_time(), charset])
    for item in item_list:
        data = head_data + item
        if item[0] == key_name:
            data.append(key_type)
            # insert index table
            insert('index', [db_name, table_name, False, key_type, key_name, data[-5], 'btree'])
        else:
            data.append('')

        # check unique

        insert('columns', data)

    # print("[INFO] create table successfully")


def proc_insert_data(db_name, tokens, insert_op=True):
    """
    INSERT INTO table_name values(a,b,c,d)
    :param insert_op:
    :param db_name:
    :param tokens:
    :return:
    """
    root = tokens[0] == "sudo"
    table_name = tokens[3] if root else tokens[2]
    old_row_time = select('tables', ['table_rows', 'update_time'], where_key=['table_schema', 'table_name'],
                          where_value=[db_name, table_name])

    if root:
        if insert_op:
            # really insert data into tables
            vid = tokens.index('values')
            vid += 2
            values = tokens[vid:-2]
            values = [e for e in values if e not in [',']]
            insert(table_name, values)
            print("[INFO] <sudo> insert data successfully")
            return
        else:
            where_data = None
            where_idx = len(tokens)
            if 'where' in tokens:
                where_idx = tokens.index('where')
                where_data = tokens[1 + where_idx:-1]
            from_table = tokens[1 + tokens.index('from'):where_idx]
            where_keys = []
            where_values = []
            if where_data:
                where_data = "".join(where_data)
                for wd in where_data.split("and"):
                    wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
                    key = wd.split("=")[0]
                    value = wd.split("=")[1]
                    where_keys.append(key)
                    where_values.append(value)
            delete(table_name, key=where_keys, value=where_values)
            print("[INFO] <sudo> delete data successfully")
            return
    if len(old_row_time) == 0:
        error(f'db [{db_name}] table [{table_name}] does not exits!')
        return

    # def update(table_name, key, key_value, new_value):
    dt = 1 if insert_op else -1
    update('tables', column_name='table_rows', column_value=int(old_row_time['table_rows']) + dt,
           where_key='table_name',
           where_value=table_name)
    update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
           where_value=table_name)
    print("[INFO] insert/delete data successfully")


def proc_update_data(db_name, tokens):
    # only change the update time
    table_name = tokens[1]
    ret = update('tables', column_name='update_time', column_value=get_now_time(), where_key='table_name',
                 where_value=table_name)
    if not ret:
        error(f'db [{db_name}] table [{table_name}] does not exits!')
        return
    print("[INFO] update data successfully")


def proc_delete_data(db_name, tokens):
    """
    DELETE FROM runoob_tbl WHERE runoob_id=3;
    :param db_name:
    :param tokens:
    :return:
    """
    proc_insert_data(db_name=db_name, tokens=tokens, insert_op=False)


def proc_drop(db_name, tokens):
    # 删除字段
    table_name = tokens[tokens.index('table') + 1]
    column_name = tokens[tokens.index('drop') + 2]
    ret = delete(table_name='columns', key=['table_name', 'column_name'], value=[table_name, column_name])
    if not ret:
        error(f'db [{db_name}] table [{table_name}] column [{column_name}] does not exits!')
        return
    print("[INFO] drop column successfully")


def proc_add_column(db_name, tokens):
    # ALTER  TABLE  table_name  ADD  i  int
    # 修改和删除字段的默认值
    table_name = tokens[tokens.index('table') + 1]
    i = tokens.index('add') + 2
    column_name = tokens[i]
    column_type = tokens[i + 1]
    i += 2
    # table_schema, table_name, column_name, ordinal_position, default_value, nullable, data_type, max_char_length,
    # auto_increment, column_key
    default_value = ''
    nullable = True
    data_type = ''
    max_char_length = ''
    auto_increment = False
    column_key = ''
    auto_increment_idx = ''
    if tokens[i] == 'not' and tokens[i + 1] == 'null':
        nullable = False
        i += 2

    if 'auto_increment' in tokens:
        auto_increment = True
        idx = tokens.index('auto_increment')
        if tokens[idx + 1] == '=':
            auto_increment_idx = tokens[idx + 2]

    if 'default' in tokens:
        default_value = tokens[tokens.index('default') + 1]

    last_ordinal_pos = select('columns', 'ordinal_position', where_key=['table_schema', 'table_name'],
                              where_value=[db_name, table_name]).max()

    data = [db_name, table_name, column_name, float(last_ordinal_pos) + 1, default_value, nullable, data_type,
            max_char_length,
            auto_increment, column_key]
    insert('columns', data)
    print("[INFO] add column successfully")


def proc_add_key(db_name, tokens):
    table_name = tokens[tokens.index('table') + 1]
    i = tokens.index('add') + 1
    idx = tokens.index('(')
    column_name = tokens[idx + 1]
    nullable = select('columns', raw_attributes=['nullable'],
                      where_key=['table_schema', 'table_name', 'column_name'],
                      where_value=[db_name, table_name, column_name]).values[0, 0]

    if tokens[i] in ['index', 'unique']:
        # 添加索引
        # ALTER TABLE tb_name ADD INDEX [<索引名>] [<索引类型>] (<列名>,…)
        non_unique = False if tokens[i] == 'unique' else True
        i += 1
        index_name = tokens[i]
        index_type = 'btree'
        i += 1
        if tokens[i] != '(':
            index_type = tokens[i]
            i += 1
        #     table_schema,table_name,non_unique,index_name,column_name,nullable,index_type

        ret = insert('index', [db_name, table_name, non_unique, index_name, column_name, nullable, index_type],
                     unique_columns=['table_schema', 'index_name', 'table_name'],
                     unique_values=[db_name, index_name, table_name])
        if not ret:
            error(f'db [{db_name}] index [{index_name}] already exits!')
            return

    elif tokens[i] == 'primary':
        #     ADD PRIMARY KEY [<索引类型>] (<列名>,…)
        i += 2
        index_type = 'btree'
        if tokens[i] != '(':
            index_type = tokens[i]
            i += 1
        ret = insert('index', [db_name, table_name, False, 'primary', column_name, nullable, index_type],
                     unique_columns=['table_schema', 'index_name', 'table_name'],
                     unique_values=[db_name, 'primary', table_name])
        if not ret:
            error(f'db [{db_name}] index [PRIMARY] already exits!')
            return

    print("[INFO] add key successfully")


def proc_change(db_name, tokens):
    table_name = tokens[2]
    column_name = tokens[4]
    new_column_name = tokens[5]
    i = 6
    if len(tokens) > 6:
        column_type = tokens[i]
        update('columns', column_name='data_type', column_value=column_type,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
        i += 2
    max_len = ''
    if '(' in tokens:
        max_len = tokens[i]
        i += 1
    update('columns', column_name='max_char_length', column_value=max_len,
           where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
    if i + 1 < len(tokens) and [i] == 'not' and tokens[i + 1] == 'null':
        nullable = False
    if 'auto_increment' in tokens:
        auto_increment = True
        update('columns', column_name='auto_increment', column_value=auto_increment,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])
    if 'default' in tokens:
        default_value = tokens[tokens.index('default') + 1]
        update('columns', column_name='default_value', column_value=default_value,
               where_key=['table_schema', 'table_name', 'column_name'], where_value=[db_name, table_name, column_name])

    ret = update('columns', column_name='column_name', column_value=new_column_name,
                 where_key=['table_schema', 'table_name', 'column_name'],
                 where_value=[db_name, table_name, column_name])
    if not ret:
        error(f'db [{db_name}] table [{table_name}] column [{column_name}] does not exits!')
        return

    print("[INFO] alter table successfully")


def proc_modify(db_name, tokens):
    column_name = tokens[4]
    tokens.insert(4, column_name)
    proc_change(db_name, tokens)


def proc_drop_key(db_name, tokens):
    table_name = tokens[2]
    # print(tokens)
    index_name = ''
    if 'index' in tokens:
        index_name = tokens[tokens.index('index') + 1]
    elif 'primary' in tokens:
        index_name = 'primary'
    elif 'unique' in tokens:
        index_name = tokens[tokens.index('unique') + 1]
    ret = delete('index', key=['table_schema', 'table_name', 'index_name'], value=[db_name, table_name, index_name])
    if not ret:
        error(f'db [{db_name}] table [{table_name}] index [{index_name}] does not exits!')
        return
    print("[INFO] alter table successfully")


def proc_alter(db_name, tokens):
    # drop
    if 'drop' in tokens and 'column' in tokens:
        proc_drop(db_name, tokens)
    elif 'drop' in tokens:
        proc_drop_key(db_name, tokens)
    elif 'add' in tokens and 'column' in tokens:
        proc_add_column(db_name, tokens)
    elif 'add' in tokens:
        proc_add_key(db_name, tokens)
    elif 'change' in tokens:
        proc_change(db_name, tokens)
    elif 'modify' in tokens:
        proc_modify(db_name, tokens)


def proc_drop_db(tokens):
    db_name = tokens[2]
    ret = delete('schema', key='db_name', value=db_name)
    if not ret:
        error(f'db [{db_name}] does not exits!')
        return
    delete('columns', key='table_schema', value=db_name)
    delete('tables', key='table_schema', value=db_name)
    delete('index', key='table_schema', value=db_name)
    print("[INFO] drop database successfully")


def proc_drop_table(db_name, tokens):
    table_name = tokens[2]
    ret = delete('tables', key=['table_schema', 'table_name'], value=[db_name, table_name])
    if not ret:
        error(f'db [{db_name}] table [{table_name}] does not exits!')
        return
    delete('columns', key=['table_schema', 'table_name'], value=[db_name, table_name])
    delete('index', key=['table_schema', 'table_name'], value=[db_name, table_name])
    print("[INFO] drop table successfully")


import re


def proc_select_data(db_name, tokens):
    """
    select * from table where table_name='test'
    select column_name, data_type from columns where table_name='tasks' and column_name='subject'
    :param db_name:
    :param tokens:
    :return:
    """

    from_idx = tokens.index('from')
    attributes = tokens[1:from_idx]
    attributes = [e for e in attributes if e != ',']
    where_data = None
    where_idx = len(tokens)
    if 'where' in tokens:
        where_idx = tokens.index('where')
        where_data = tokens[1 + where_idx:-1]
    from_table = tokens[1 + from_idx:where_idx]
    where_keys = []
    where_values = []
    if where_data:
        where_data = "".join(where_data)
        for wd in where_data.split("and"):
            wd = re.sub('r[\"\']', '', wd.replace('\'', ''))
            key = wd.split("=")[0]
            value = wd.split("=")[1]
            where_keys.append(key)
            where_values.append(value)
    data = select(from_table[0], raw_attributes=attributes, where_key=where_keys, where_value=where_values)
    if len(data) == 0:
        print('None')
    else:
        print(data)


def proc_show(db_name, tokens):
    if tokens[1] == 'databases':
        parse(f'select db_name from schema;')
    elif tokens[1] in ['tables', 'columns', 'index']:
        ans = select_by_schema(tokens[1], db_name)
        print(ans)


db_name = ''


def parse(cmd: str):
    import re
    tokens = re.split(r"([ ,();=])", cmd.lower().strip())
    tokens = [t for t in tokens if t not in [' ', '', '\n']]
    global db_name
    i = 0
    if tokens[0] == "sudo": i += 1
    if tokens[i] == 'use':
        db_name = tokens[i + 1]
        ret = select('schema', ['*'], where_key=['db_name'], where_value=[db_name])
        if len(ret) == 0:
            error(f"[ERROR] database [{db_name}] does not exits")
            return

        print('change database to', db_name)
    if tokens[i] == 'create' and tokens[i + 1] == 'database':
        proc_create_db(tokens)
    if tokens[i] == 'create' and tokens[i + 1] == 'table':
        proc_create_table(db_name, tokens)
    if tokens[i] == 'insert':
        proc_insert_data(db_name, tokens)
    if tokens[i] == 'update':
        proc_update_data(db_name, tokens)
    if tokens[i] == 'delete':
        proc_delete_data(db_name, tokens)
    if tokens[i] == 'alter':
        proc_alter(db_name, tokens)
    if tokens[i] == 'drop' and tokens[i + 1] == 'database':
        proc_drop_db(tokens)
    if tokens[i] == 'drop' and tokens[i + 1] == 'table':
        proc_drop_table(db_name, tokens)
    if tokens[i] == 'select':
        proc_select_data(db_name, tokens)
    if tokens[i] == 'show':
        proc_show(db_name, tokens)
    if tokens[i] == 'database':
        print(db_name)


import time

if __name__ == '__main__':
    # table_schema, table_name, table_type, engine, table_rows, create_time,auto_increment,update_time,table_collation
    # insert('tables', ['test_db', 'test', 'base_table', 'innodb', 10, '2019-12-21', 11, '2020-12-12', 'utf8'])
    # info = select('tables', ['table_name', 'update_time'])
    # delete('tables', key='table_name', value='test')
    # print(info)
    # update('tables', 'table_name', 'test', 'test_new')
    # info = select('tables', ['table_name', 'update_time'])
    # delete("columns", key='table_name', value='tasks')
    # parse("create database test_db;")
    file = open('sql_test', encoding='utf8')
    cmd = file.read()
    # avg_time = 0
    # parse("use test_ef_db;")
    # for i in range(10):
    #     st = time.time()
    #     cmd = " select * from columns where table_name=task506.0;"
    #     parse(cmd)
    #     print(time.time()-st)
    #     avg_time += (time.time()-st)/10
    # print(avg_time)
    # parse("use test_ef_db")
    # for i in np.arange(1e7):
    #     cmd = "create table task" + str(i) + "(idx INT not null) primary key(idx);"
    #     parse(cmd)

    # cmd = "create database test_db"
    # cmd = "create table test_table"
    # info = select('columns', '*')
    # print(info)
    # cmd = "insert into test values(1,2,3)"
    # cmd = "UPDATE test SET name='szj' WHERE sid=20180001;"
    # cmd = "DELETE FROM tasks WHERE sid=20184376;"
    # cmd = "ALTER TABLE tasks DROP description;"
    # cmd = "ALTER TABLE tasks add new_column int not null;"
    # cmd = "ALTER TABLE tasks change description my_column varchar(128) not null;"
    # cmd = "ALTER TABLE tasks modify my_column int not null default -1;"
    # cmd = "create database temp_db";
    # cmd = "DROP DATABASE temp_db;"
    # cmd = "ALTER TABLE tasks ADD INDEX idx_name hash (task_id);"
    # cmd = "ALTER TABLE tasks ADD UNIQUE idx_name hash (task_id);"
    # cmd = "ALTER TABLE tasks ADD UNIQUE idx_name hash (task_id);"
    # cmd = "ALTER TABLE tasks ADD PRIMARY KEY btree (task_id);"
    # cmd = "ALTER TABLE tasks DROP INDEX idx_name;"
    # cmd = "ALTER TABLE tasks DROP PRIMARY KEY btree (task_id);"

    # cmd = "DROP TABLE tasks;"
    # cmd = "DROP DATABASE test_db;"
    # cmd = "select * from table where table_name='test'"
    # cmd = "select * from columns where table_name = 'tasks'"
    # cmd = "select column_name, data_type from columns where table_name='tasks' and column_name='subject';"

    # cmd = "use test_db;"
    # parse(cmd)
    # info = select('columns', '*')
    # print(info)
    cmd_list = []

    while True:
        cmd = input("command?  ")
        if cmd == 'quit;':
            break
        cmd_list.append(cmd)
        while ';' not in cmd:
            cmd = input('> ')
            cmd_list.append(cmd)
        cmd = " ".join(cmd_list)
        cmd_list = []
        st = time.time()
        parse(cmd)
        # print('cost', time.time() - st)
