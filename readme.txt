
Qinghe,2021,<zhaopinglu77@gmail.com>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A tool to migrate schema and data from Oracle 11g+ to MySQL 5.7+.
Feel free to use this software and there is no warranty for it.

Change Logs:
0.9.3 - Fixed a few issues.
0.9.2 - In incremental mode, add support for handling deleted data since last consistent mode exporting.
0.9.1 - Add consistent/incremental exporting mode.
0.9.0 - Add parallel exporting.
0.8.0 - initial release.

How to use this tool:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Install instant client 19.3 from the following link if necessary.
https://yum.oracle.com/repo/OracleLinux/OL7/oracle/instantclient/x86_64/getPackage/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm

Note: The instant client 19.3 version can work with 11.2.0.4+.

2. Check detail help information with the following command:
./qinghe -h


3. Clean up garbage data for example log tables in source oracle database.
i.e.:
truncate table QRTZ_ERROR_LOG;
truncate table T_LOG_EXCEPTION_STACK;

3.1 In Oracle fix data containing tailing whitespace which will cause unique-violation in MySQL
i.e.:
update T_I18N_STATIC set text_key=trim(text_key)||'_' where text_key like '% ';


4. Export schema ddl and data from Oracle database.

# Export ddl
./qinghe -h 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c metadata

# Export data in normal mode.
./qinghe -h 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data

# Export data in consistent mode.
./qinghe -h 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data -m consistent

# Export data in incremental mode.
./qinghe -h 192.168.0.1 -s orcl -S myschema -u myuser -p mypass -c data -m incremental

Noe: The generated files can be found in the output directory.


5. Create database on target MySQL database.
create DATABASE `myschema` DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_bin;

Note: Sometimes, need to use collate utf8mb4_bin to avoid unique key conflict.


6. Import schema ddl and data into MySQL database.
mysql -uroot -proot myschema < myschema_ddl.sql
mysql -uroot -proot --binary-mode myschema < myschema_data.sql 2>&1 | tee myschema_data.log

Note: need to use binary-mode when importing data, to avoid error caused by char '\0' in some 'text' data.



Limitations & Issues:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
0. Use system when exporting data from Oracle 11.2.0.1, otherwise might hit error ORA-29491 during exporting partition tables.

1. Can not create fk on partial-key index. This is MySQL's limitation, not qinghe's.

2. Some original T_I18N_STATIC.TEXT_KEY data contain tailing whitespace.
This is fine in Oracle, but will cause the unique key violation in MySQL.
Because MySQL will discard tailing whitespace when indexing key value.

3. While exporting in incremental mode, if a table was created after previous consistent/incremental export,
then Qinghe will skip to generate delete sql for this table due to the error as below which is fine.
ORA-01466: unable to read data - table definition has changed.





Fix:
update T_I18N_STATIC set text_key=concat(trim(text_key),'_') where text_key like '% ';
alter table T_I18N_STATIC add  UNIQUE KEY `AK_TKLC_T_I18N_S` (`LANGUAGE_CODE`,`TEXT_KEY`);

3. Be careful, when configuring large values for parameters batch_number or parallel,
this program might temporarily allocate large memory to store the table data.

todo:
~~~~~~~~~~~~~
2. import : initial support & resuming on break point.




Hack rust-oracle 0.5.3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
src/row.rs:


unsafe impl Sync for Row {}
unsafe impl Send for Row {}


unsafe impl<'a, T> Sync for ResultSet<'a, T> where T: RowValue {}
unsafe impl<'a, T> Send for ResultSet<'a, T> where T: RowValue {}

