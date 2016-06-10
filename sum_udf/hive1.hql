-----------------------------------------------------------
------------------ Hive UDF example -----------------------
-----------------------------------------------------------

ADD JAR ./target/sum_udf-1.0.jar;

CREATE TEMPORARY FUNCTION sum_udf AS 'org.tkorostelev.sum_udf.SumUDF';
