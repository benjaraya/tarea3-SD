-- Cargar datos desde el archivo charlie_data4.csv
data = LOAD '/user/hadoop/charlie_data3/charlie2.csv' USING PigStorage(',') AS (
    branch_addr:chararray,
    branch_type:chararray,
    taken:int,
    target:chararray
);

-- Contar el número total de registros
total_count = FOREACH (GROUP data ALL) GENERATE COUNT(data) AS total_count;

-- Almacenar el número total de registros
STORE total_count INTO '/user/hadoop/output/total_records00' USING PigStorage('\t');

-- Contar la frecuencia de cada tipo de branch
branch_type_frequency = FOREACH (GROUP data BY branch_type) GENERATE 
    group AS branch_type, 
    COUNT(data) AS frequency;

-- Almacenar la frecuencia de cada tipo de branch
STORE branch_type_frequency INTO '/user/hadoop/output/branch_type_frequency00' USING PigStorage('\t');

-- Analizar la relación entre branch_type y taken
branch_type_taken_relationship = FOREACH (GROUP data BY (branch_type, taken)) GENERATE
    group.branch_type AS branch_type, 
    group.taken AS taken, 
    COUNT(data) AS count;

-- Almacenar la relación entre branch_type y taken
STORE branch_type_taken_relationship INTO '/user/hadoop/output/branch_type_taken_relationship00' USING PigStorage('\t');

-- Calcular la proporción de registros con taken = 1 para cada branch_type

-- Filtrar los registros donde taken = 1
taken_1 = FILTER data BY taken == 1;

-- Calcular el número de registros donde taken = 1 por branch_type
grouped_taken_1 = GROUP taken_1 BY branch_type;
branch_counts_taken_1 = FOREACH grouped_taken_1 GENERATE
    group AS branch_type,
    COUNT(taken_1) AS count_taken_1;

-- Calcular el total de registros por branch_type
grouped_total = GROUP data BY branch_type;
branch_counts_total = FOREACH grouped_total GENERATE
    group AS branch_type,
    COUNT(data) AS total_records;

-- Unir los resultados para calcular la proporción
proportions = JOIN branch_counts_taken_1 BY branch_type, branch_counts_total BY branch_type;

-- Calcular la proporción
branch_type_taken_proportion = FOREACH proportions GENERATE
    branch_counts_taken_1::branch_type AS branch_type,
    branch_counts_taken_1::count_taken_1 AS count_taken_1,
    branch_counts_total::total_records AS total_records,
    (double)branch_counts_taken_1::count_taken_1 / (double)branch_counts_total::total_records AS proportion_taken_1;

-- Almacenar la proporción de registros con taken = 1 para cada branch_type
STORE branch_type_taken_proportion INTO '/user/hadoop/output/branch_type_taken_proportion00' USING PigStorage('\t');

