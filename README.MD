*Oracle2ClickHouse* 是一个将 Oracle 表结构转成 ClickHouse 下的语句，及生成Select 导出数据语句的工具

- 可以指定某个库甚至某一个表进行SQL转换，但可能需要手工去进行调试
- 同时会生成Select导出数据，手工执行并导出CSV格式数据，再手工导入到ClickHouse


配置文件
---
```
testkey=1
#Oracle用户名
oracleUser=O_username
#Oracle密码
oraclePwd=myPassword
#jdbc配置
oracleUrl=jdbc:oracle:thin:@10.60.2.52:1521:databaseName

#导出哪一个库的表结构
database=DataBaseName
#导出哪一个表，如果这个没有配置，则将整个库的表全部转换
tablename=tableName

#转换成功的sql 放在哪个文件，按 database/table这样的目录结构保存
datadir=E:/oracel_data
```


sql文件
---

**.Distributed.frm.sql*  Distributed表 DDL sql语句

**.frm.sql*  DDL sql语句

**.export.sql*  导出数据的语句

编译
---
```
mvn install -Dmaven.test.skip=true

```

备注
---

- 这个工具只是帮助导出 ddl 语句，并不是帮忙直接将数据导到ClickHouse
- 并且生成的sql语句不是完全符合您的需求，可以手动修改一下再使用
- DateTime类型字段要特别注意，Oracle里可能不能为Null，并实际存储的又是Null
