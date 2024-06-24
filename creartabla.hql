-- Crear tabla externa charlie_data3 en Hive
DROP TABLE IF EXISTS charlie_data3;
CREATE EXTERNAL TABLE charlie_data3 (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/charlie_data3';  -- Ubicación del directorio en HDFS



