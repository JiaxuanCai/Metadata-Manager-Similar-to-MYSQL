## Metadata manager implementation

collaborator: Runze Tian, ifrozenwhale

## Implementation scheme

- Use CSV files to store table structures
- The Python and PANDAS command is used to read and write CSV files. The dataframe is used as the data structure to add, delete, and modify

## Design

### table construction

- columns table

  ```csv
  table_schema,table_name,column_name,ordinal_position,default_value,nullable,data_type,max_char_length,auto_increment,column_key
  ```

  

- schema table

  ```
  db_name,charset,collate
  ```

- tables table

  ```
  table_schema,table_name,table_type,engine,table_rows,create_time,auto_increment,update_time,table_collation
  test_db,tasks,base_table,innodb,0,2021-05-05 17:43:18,,2021-05-05 17:43:18,utf8
  ```

- index table

- views table

- user_privileges table

- schema_privileges table

- table_privileges table

- constraint table

- triggers table


## SQL parsing

### Create

- Create database

  ```mysql
  create database test_db
  ```

- Create data table

  format:

  ```
  CREATE TABLE < TABLE name > ([TABLE definition options])[TABLE options][partition options];
  
  [Table definition options]: < column name 1> < type 1> [,... < column n> < type n>
  ```

  ```mysql
  CREATE TABLE tasks (
    task_id INT(11) NOT NULL AUTO_INCREMENT,
    subject VARCHAR(45) DEFAULT NULL,
    start_date DATE DEFAULT NULL,
    end_date DATE DEFAULT NULL,
    description VARCHAR(200) DEFAULT NULL,
    PRIMARY KEY (task_id)
  ) ENGINE=innodb,CHARSET=utf8;
  ```

  **Repeat check** : Error when already exists:

  ```mysql
  [ERROR] db [db_name] table [table_name] already exists!
  ```

  

### Select

- **Field** query、**where** clause multi conditions query

  ```mysql
  select * from table where table_name='test'
  select column_name, data_type from columns where table_name='tasks' and column_name='subject'
  ```

### Insert

- Insert statement only affects the `table_rows`property and the`update_time property, so no parsing is done here

  ```mysql
  insert into test values(1,2,3)
  ```

### Update

- Change the field name

  ```mysql
  UPDATE test SET name='szj' WHERE sid=20180001;
  ```

  **Existence check**：Update a table or field that does not exist will result in error

  ```mysql
  [ERROR] db [db_name] table [table_name] column [column_name] does not exist!	
  ```

  

### Delete

- The impact of the Delete statement is the same as that of the INSERT statement

  ```mysql
  DELETE FROM tasks WHERE sid=20184376;
  ```

### Drop Database

- drop database

  ```mysql
  DROP DATBASE DB_NAME;
  ```

### Alter

#### Drop

- Drop a field of a table

  ```mysql
  ALTER TABLE tasks DROP COLUMN description;
  ```

#### Add

- Add a field to a table

  ```mysql
  ALTER TABLE tasks ADD COLUMN new_column int not null;
  ```

#### Change

- Modify field names and attributes

  ```mysql
  ALTER TABLE tasks change description my_column varchar(128) not null;		
  ```

#### Modify

- Modify field attributes

  ```mysql
  ALTER TABLE tasks modify my_column int not null default -1
  ```

### Operations on indexes and constraints

- Add index:

  Unique index

  ```mysql
  ALTER  TABLE  table_name  ADD  UNIQUE (column_name)
  ```

  Normal index

  ```mysql
  ALTER  TABLE  test  ADD  INDEX index_name (column_name)
  ```

- Delete index

  ```mysql
  ALTER TABLE table_name DROP INDEX index_name
  ```

  ```mysql
  ALTER TABLE table_name DROP PRIMARY KEY
  ```

### Others

- Optimized SQL syntax

  ```mysql
  show index;
  show tables;
  show columns;
  ```

- Support multi-line input to; As a closing sign

- Existence check, error message

- Synchronous deletion

- Switch the current database using 'use database'

