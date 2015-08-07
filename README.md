# hive-json-serde
hive的Array 解析json中的数组、Map解析json中的对象：
{"pluginList" : [{"name" : "1","browser" : "1", "on" : "2" },
 {"name" : "1",   "browser" : "3","on" : "2" }]}
create table sql：
create table test_json (
pluginlist Array<Map<String,String>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
WITH SERDEPROPERTIES(
"pluginlist"="$.pluginList"
)

select sql：
select pluginlist[0],pluginlist[0]['name']
from src_b5t_level2plugininfo
limit 10;

