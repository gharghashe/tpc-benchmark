CREATE TABLE store.PART (

	P_PARTKEY		SERIAL PRIMARY KEY,
	P_NAME			VARCHAR(55),
	P_MFGR			CHAR(25),
	P_BRAND			CHAR(10),
	P_TYPE			VARCHAR(25),
	P_SIZE			INTEGER,
	P_CONTAINER		CHAR(10),
	P_RETAILPRICE	DECIMAL,
	P_COMMENT		VARCHAR(23),
	test			VARCHAR(23)
);
COPY store.PART(P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT,test) FROM '/data/OLAP_Benchmark_data/part.tbl' DELIMITER '|';

CREATE TABLE store.SUPPLIER (
	S_SUPPKEY		SERIAL PRIMARY KEY,
	S_NAME			CHAR(25),
	S_ADDRESS		VARCHAR(40),
	S_NATIONKEY		BIGINT NOT NULL,
	S_PHONE			CHAR(15),
	S_ACCTBAL		DECIMAL,
	S_COMMENT		VARCHAR(101),
	test			VARCHAR(23)
);
COPY store.SUPPLIER(S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT,test) FROM '/data/OLAP_Benchmark_data/supplier.tbl' DELIMITER '|';

CREATE TABLE store.PARTSUPP (
	PS_PARTKEY		BIGINT NOT NULL,
	PS_SUPPKEY		BIGINT NOT NULL,
	PS_AVAILQTY		INTEGER,
	PS_SUPPLYCOST	DECIMAL,
	PS_COMMENT		VARCHAR(199),
	PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
	test			VARCHAR(23)
);
COPY store.PARTSUPP(PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT,test) FROM '/data/OLAP_Benchmark_data/partsupp.tbl' DELIMITER '|';

CREATE TABLE store.CUSTOMER (
	C_CUSTKEY		SERIAL PRIMARY KEY,
	C_NAME			VARCHAR(25),
	C_ADDRESS		VARCHAR(40),
	C_NATIONKEY		BIGINT NOT NULL,
	C_PHONE			CHAR(15),
	C_ACCTBAL		DECIMAL,
	C_MKTSEGMENT	CHAR(10),
	C_COMMENT		VARCHAR(117)
);

COPY store.CUSTOMER(C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT,test) FROM '/data/OLAP_Benchmark_data/customer.tbl' DELIMITER '|';

DROP TABLE IF EXISTS ORDERS CASCADE;
CREATE TABLE store.ORDERS (
	O_ORDERKEY		SERIAL PRIMARY KEY,
	O_CUSTKEY		BIGINT NOT NULL,
	O_ORDERSTATUS	CHAR(1),
	O_TOTALPRICE	DECIMAL,
	O_ORDERDATE		DATE,
	O_ORDERPRIORITY	CHAR(15),
	O_CLERK			CHAR(15),
	O_SHIPPRIORITY	INTEGER,
	O_COMMENT		VARCHAR(79),
	test			VARCHAR(23)
);

COPY store.ORDERS(O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT,test) FROM '/data/OLAP_Benchmark_data/orders.tbl' DELIMITER '|';

DROP TABLE IF EXISTS LINEITEM CASCADE;
CREATE TABLE store.LINEITEM (
	L_ORDERKEY		BIGINT NOT NULL,
	L_PARTKEY		BIGINT NOT NULL,
	L_SUPPKEY		BIGINT NOT NULL,
	L_LINENUMBER	INTEGER,
	L_QUANTITY		DECIMAL,
	L_EXTENDEDPRICE	DECIMAL,
	L_DISCOUNT		DECIMAL,
	L_TAX			DECIMAL,
	L_RETURNFLAG	CHAR(1),
	L_LINESTATUS	CHAR(1),
	L_SHIPDATE		DATE,
	L_COMMITDATE	DATE,
	L_RECEIPTDATE	DATE,
	L_SHIPINSTRUCT	CHAR(25),
	L_SHIPMODE		CHAR(10),
	L_COMMENT		VARCHAR(44),
	test			VARCHAR(23)
	PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);

COPY store.LINEITEM(L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT,test) FROM '/data/OLAP_Benchmark_data/lineitem.tbl' DELIMITER '|';

DROP TABLE IF EXISTS NATION CASCADE;
CREATE TABLE store.NATION (
	N_NATIONKEY		SERIAL PRIMARY KEY,
	N_NAME			CHAR(25),
	N_REGIONKEY		BIGINT NOT NULL,
	N_COMMENT		VARCHAR(152),
	test			VARCHAR(23)
);

COPY store.NATION(N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT,test) FROM '/data/OLAP_Benchmark_data/nation.tbl' DELIMITER '|';

DROP TABLE IF EXISTS REGION CASCADE;
CREATE TABLE store.REGION (
	R_REGIONKEY	SERIAL PRIMARY KEY,
	R_NAME		CHAR(25),
	R_COMMENT	VARCHAR(152),
	test			VARCHAR(23)
);

COPY store.REGION(R_REGIONKEY,R_NAME,R_COMMENT) FROM '/data/OLAP_Benchmark_data/region.tbl' DELIMITER '|';

ALTER TABLE store.LINEITEM DROP COLUMN test ;
ALTER TABLE store.NATION DROP COLUMN test ;
ALTER TABLE store.ORDERS DROP COLUMN test ;
ALTER TABLE store.CUSTOMER DROP COLUMN test ;
ALTER TABLE store.PARTSUPP DROP COLUMN test ;
ALTER TABLE store.SUPPLIER DROP COLUMN test ;
ALTER TABLE store.PART DROP COLUMN ;