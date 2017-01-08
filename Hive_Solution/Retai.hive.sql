-- Start hadoop Command : start-all.sh

-- Copy your csv-serde-1.1.2-0.11.0-all.jar into you hadoop /home/user file command : hadoop fs -put ~/csv-serde-1.1.2-0.11.0-all.jar /home/user/

-- if in HDFS /home/user/ Directory in not present Create directory into HDFS Command : hadoop fs -mkdir /home/user/ 

-- Start hive Command : hive

-- Run this command 

CREATE DATABASE HIVEPROJECT;

USE HIVEPROJECT;

add jar csv-serde-1.1.2-0.11.0-all.jar;

DROP TABLE IF EXISTS CUST_TRANSACTION;

CREATE TABLE CUST_TRANSACTION 
(
 TRX_DATE VARCHAR(100)
,CUST_ID VARCHAR(30)
,AGE_BAND VARCHAR(2)
,RED_AREA VARCHAR(10)
,SUBCLASS VARCHAR(50)
,PRD_ID VARCHAR(50)
,QTY DECIMAL(10,3)
,COST DECIMAL(10,3)
,NET_AMT DECIMAL(10,3)
)
COMMENT 'CUST_TRANSACTION'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/Input_Retails/D01' INTO TABLE CUST_TRANSACTION;

LOAD DATA INPATH '/Input_Retails/D02' INTO TABLE CUST_TRANSACTION;

LOAD DATA INPATH '/Input_Retails/D11' INTO TABLE CUST_TRANSACTION;

LOAD DATA INPATH '/Input_Retails/D12' INTO TABLE CUST_TRANSACTION;


--Find out the customer I.D for the customer who has spent the maximum amount in a month and a year. Let’s do this for year on year basis as well. 
/*

2000	11	02131269  	109358.000
2000	12	01558418  	126209.000
2001	01	00842419  	81140.000
2001	02	01622362  	444328.000

 */
 select B.TRX_YEAR,B.TRX_MONTH,B.CUST_ID,B.NET_AMT from (
select CUST.TRX_YEAR,CUST.TRX_MONTH,CUST.CUST_ID,CUST.NET_AMT,rank() over (partition by CUST.TRX_YEAR,CUST.TRX_MONTH order by CUST.NET_AMT desc) rnk
 from(
select SUBSTR(CUST_TRANSACTION.TRX_DATE,0,4) TRX_YEAR,SUBSTR(CUST_TRANSACTION.TRX_DATE,6,2) TRX_MONTH,CUST_TRANSACTION.CUST_ID CUST_ID,SUM(CUST_TRANSACTION.NET_AMT) NET_AMT from CUST_TRANSACTION 
group by SUBSTR(CUST_TRANSACTION.TRX_DATE,0,4),SUBSTR(CUST_TRANSACTION.TRX_DATE,6,2),CUST_TRANSACTION.CUST_ID) CUST
) B where B.rnk=1;

--By year
/*
2000	01558418  	202091.000
2001	01622362  	444526.000

*/
select B.TRX_YEAR,B.CUST_ID,B.NET_AMT from (
select CUST.TRX_YEAR,CUST.CUST_ID,CUST.NET_AMT,rank() over (partition by CUST.TRX_YEAR order by CUST.NET_AMT desc) rnk
 from(
select SUBSTR(CUST_TRANSACTION.TRX_DATE,0,4) TRX_YEAR, CUST_TRANSACTION.CUST_ID CUST_ID,SUM(CUST_TRANSACTION.NET_AMT) NET_AMT from CUST_TRANSACTION 
group by SUBSTR(CUST_TRANSACTION.TRX_DATE,0,4), CUST_TRANSACTION.CUST_ID) CUST
) B where B.rnk=1;

--Find out the top 4 or top 10 product being sold 
/*
4714981010038	14537.000
4710421090059	11790.000
4711271000014	10615.000
4711663700010	3810.000

*/
SELECT B.PRD_ID,B.QTY FROM(
select CUST_TRANSACTION.PRD_ID,SUM(CUST_TRANSACTION.QTY) QTY from CUST_TRANSACTION
group BY CUST_TRANSACTION.PRD_ID 
) B
ORDER by B.QTY DESC LIMIT 4;

-- Find out the top grossing product and the product subclass for the age group A, B, C etc..... 
/*
4711588210441	A 	446185.000
8712045008539	B 	250644.000
8712045008539	C 	369511.000
8712045008539	D 	510868.000
8712045008539	E 	134970.000
8712045008539	F 	106163.000
8712045008539	G 	63037.000
8712045011317	H 	69260.000
8712045000151	I 	45728.000
4710265849066	J 	37797.000
4902430493437	K 	23676.000

*/
select PRD_ID,AGE_BAND,NET_AMT FROM(
select B.PRD_ID,B.AGE_BAND,B.NET_AMT,rank() over (partition by B.AGE_BAND order by B.NET_AMT desc) rnk
 from(
select PRD_ID,AGE_BAND,SUM(NET_AMT) NET_AMT from CUST_TRANSACTION
group BY AGE_BAND,PRD_ID) B
) C where C.rnk=1; 