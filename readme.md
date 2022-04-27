
# Qinghe
-- zhaopinglu77@gmail.com, 2021.

A tool to migrate schema and data from Oracle 11G/12C/19C/... database to MySQL 5/8 database.

Feel free to use this software and there is no warranty for it.

# Important features
* Generates MySQL-compatible DDL for specified Oracle Schema.
* Generates MySQL-compatible Insert/Delete SQL for specified Oracle Schema data.
* Supports exporting data in parallel.
* Supports exporting data in normal/consistent/incremental mode.
* DBA-friendly customization.

# Change Logs:
* 0.9.5 - Improve help message and some other minor changes.
* 0.9.3 - Fixed a few issues.
* 0.9.2 - In incremental mode, add support for handling deleted data since last consistent mode exporting.
* 0.9.1 - Add consistent/incremental exporting mode.
* 0.9.0 - Add parallel exporting.
* 0.8.0 - initial release.

# Dependencies:
* Oracle Instant Client.

# How-to:
### 1. Install instant client 19.3 from the following link if necessary.
https://yum.oracle.com/repo/OracleLinux/OL7/oracle/instantclient/x86_64/getPackage/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm

#### Note: The instant client 19.3 version can work with 11.2.0.4+.

### 2. Check detail help information with the following command:
./qinghe -h
```
$ ./qinghe -h
Qinghe 0.9.5
Zhaoping Lu <zhaopinglu77@gmail.com>
A tool to migrate schema and data from Oracle 11g+ to MySQL 5/8.
Feel free to use this software and there is no warranty for it.

USAGE:
    qinghe [FLAGS] [OPTIONS]

FLAGS:
        --debug      Activate debug mode
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --batch-number <batch-number>
            Specify the number of value clauses for the generated multiple-row-syntax INSERT statements. 0: means no
            limit [default: 200]
    -c, --content <content>                          Content. Valid values are: metadata, data, all [default: metadata]
    -H, --host <host>                                Host [default: 192.168.12.5]
    -l, --log-level <log-level>
            Log level. Valid values: error, warn, info, debug, trace [default: info]

    -m, --mode <mode>
            Consistent mode. Only meaningful if content="data".
             # Valid values:
             * `normal`: Export all tables' data as they are;
             * `consistent`: Export all table data in a consistent snapshot;
             * `incremental`: Export the data changed since the previous consistent or incremental export.
             # Note: consistent or increment mode could hit ORA-01555 error if don't have sufficient undo tablespace.
             [default: normal]
    -o, --output-prefix <output-prefix>
            Output file name suffix, followed by a suffix string, like "_ddl.sql" [default: <SCHEMA>_<..>]

    -x, --parallel <parallel>
            The number of parallel tasks for table data exporting.
             Valid values: 0: Auto (=cpu count). 1: No parallel. 2: Run 2 tasks for data exporting, etc,.  [default: 0]
    -p, --password <password>                        Password [default: test]
    -P, --port <port>                                Port [default: 1521]
    -S, --schema <schema>                            Schema [default: TEST]
    -s, --service-name <service-name>                Service_name [default: orcl]
    -t, --table-name-pattern <table-name-pattern>    Specify the table name pattern for exporting [default: .]
    -u, --user <user>                                User [default: test]
```
### 3. Clean up garbage data for example log tables in source oracle database.
i.e.:
truncate table QRTZ_ERROR_LOG;
truncate table T_LOG_EXCEPTION_STACK;

#### 3.1 In Oracle fix data containing tailing whitespace which will cause unique-violation in MySQL
i.e.:
update T_I18N_STATIC set text_key=trim(text_key)||'_' where text_key like '% ';

### 4. Export schema ddl and data from Oracle database.

#### Export ddl
./qinghe -H 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c metadata

#### Export data in normal mode.
./qinghe -H 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data

#### Export data in consistent mode.
./qinghe -H 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data -m consistent

#### Export data in incremental mode.
./qinghe -H 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data -m incremental

##### Noe: The generated files can be found in the output directory.


### 5. Create database on target MySQL database.
create DATABASE `myschema` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_bin;

#### Note: Sometimes, need to use collate utf8mb4_bin to avoid unique key conflict.


### 6. Import schema ddl and data into MySQL database.
mysql -uroot -proot myschema < myschema_ddl.sql

#### Load single file
mysql -uroot -proot --binary-mode myschema < myschema_data.sql 2>&1 | tee myschema_data.log

#### Load multiple files
for f in $(ls normal*.sql); do echo $f; mysql --binary-mode -h 127.0.0.1 -P4000 -uroot -pMyNewPass4! ECS_EFILE < $f; done

**Note: need to use binary-mode when importing data, to avoid error caused by char '\0' in some 'text' data.**



# Limitations & Issues:
### 1. Use system when exporting data from Oracle 11.2.0.1, otherwise might hit error ORA-29491 during exporting partition tables.

### 2. Can not create fk on partial-key index. This is MySQL's limitation, not qinghe's.

### 3. Some original T_I18N_STATIC.TEXT_KEY data contain tailing whitespace.
This is fine in Oracle, but will cause the unique key violation in MySQL.
Because MySQL will discard tailing whitespace when indexing key value.

### 4. Seeing error: ORA-01466: unable to read data - table definition has changed.
While exporting in incremental mode, if a table was created after previous consistent/incremental export,
then Qinghe will hit the above error when trying to generate delete sql for this table. 
Qinghe will skip to the next step which is generating insert sql.

### 5. Seeing error: "while scanning a block scalar, found a tab character where an indentation space is expected" }', src/ora2my/initialize_db.rs:21:53
Don't use TAB key when editing ora2my_data.yaml file or ora2my_ddl.yaml.

### 6. Some common issues when loading the generated data sql into MySQL database.
#### ERROR 1366 (HY000) at line 4: Incorrect string value: '\xE6\x81\x92\xE5\xA4\xA9...' for column 'USER_NAME' at row 1
 
Fix: before create database in MySQL, make sure to set the database variables: character_set_server=utf8mb4

##### ERROR 1071 (42000) at line 11: Specified key was too long; max key length is 3072 bytes
Fix: reduce the varchar length of the table primary key.

#### ERROR 2006 (HY000) at line 4: MySQL server has gone away
Fix: Check if MySQL server is still available.
Another possible reason is the variable max_allowed_packet is too small. So set max_allowed_packet=1024M

# Fix:
* update T_I18N_STATIC set text_key=concat(trim(text_key),'_') where text_key like '% ';
alter table T_I18N_STATIC add  UNIQUE KEY `AK_TKLC_T_I18N_S` (`LANGUAGE_CODE`,`TEXT_KEY`);

* Be careful, when configuring large values for parameters batch_number or parallel,
this program might temporarily allocate large memory to store the table data.

# Todo:
* import : initial support & resuming on break point.




# Hack rust-oracle 0.5.3
* src/row.rs:
````
unsafe impl Sync for Row {}
unsafe impl Send for Row {}

unsafe impl<'a, T> Sync for ResultSet<'a, T> where T: RowValue {}
unsafe impl<'a, T> Send for ResultSet<'a, T> where T: RowValue {}
````

