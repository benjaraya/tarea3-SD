-- Cargar datos en Hive
DROP TABLE IF EXISTS charlie_data4;
CREATE EXTERNAL TABLE charlie_data4 (
    branch_addr STRING,
    branch_type STRING,
    taken INT,
    target STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  -- Delimitador de tabulación como especificado
STORED AS TEXTFILE
LOCATION '/user/hadoop/charlie_data3';  -- Ubicación del directorio en HDFS


-- Contar el número total de registros
DROP TABLE IF EXISTS total_records;
CREATE TABLE total_records AS
SELECT COUNT(*) AS total_count
FROM charlie_data4;

-- Contar la frecuencia de cada tipo de branch
DROP TABLE IF EXISTS branch_type_frequency;
CREATE TABLE branch_type_frequency AS
SELECT branch_type, COUNT(*) AS frequency
FROM charlie_data4
GROUP BY branch_type;

-- Analizar la relación entre branch_type y taken
DROP TABLE IF EXISTS branch_type_taken_relationship;
CREATE TABLE branch_type_taken_relationship AS
SELECT branch_type, taken, COUNT(*) AS count
FROM charlie_data4
GROUP BY branch_type, taken;

-- Calcular la proporción de registros con taken = 1 para cada branch_type

DROP TABLE IF EXISTS branch_type_taken_proportion;
CREATE TABLE branch_type_taken_proportion AS
SELECT branch_type, 
       COUNT(CASE WHEN taken = 1 THEN 1 ELSE NULL END) AS count_taken_1,
       COUNT(*) AS total_records,
       COUNT(CASE WHEN taken = 1 THEN 1 ELSE NULL END) / COUNT(*) AS proportion_taken_1
FROM charlie_data4
GROUP BY branch_type;


