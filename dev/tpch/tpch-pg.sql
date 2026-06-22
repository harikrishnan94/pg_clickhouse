-- TPC-H SF=10 PostgreSQL side: native heap tables (schema `pg`), the FDW import
-- of the ClickHouse tables (schema `ch`), and the Float64 lineitem variant.
--
-- Connection is templated by the Makefile, which passes the dedicated
-- ClickHouse server's manifest ports:
--   psql -v ch_host=127.0.0.1 -v ch_http_port=<manifest CH_HTTP_PORT> ...
--
-- Standardized server: `ch_bench`, driver 'http' (one server used for BOTH FDW
-- pushdown and SHM offload). Re-runnable: only objects this script owns are
-- dropped; tpch-ch.sql must have loaded the ClickHouse side (incl. lineitem_f64)
-- first so the IMPORT picks it up as ch.lineitem_f64.

SET client_min_messages = notice;

\set ch_user default
\getenv ch_user CLICKHOUSE_USER
\set ch_pass ''
\getenv ch_pass CLICKHOUSE_PASSWORD

-- Install the planner/executor hooks for every session on this database (the
-- supported way to enable SHM offload); per-database, never global.
ALTER DATABASE tpch_sf10 SET session_preload_libraries = 'pg_clickhouse';

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_clickhouse;

DROP SERVER IF EXISTS ch_bench CASCADE;
CREATE SERVER ch_bench FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS (driver 'http', host :'ch_host', port :'ch_http_port', dbname 'tpch_sf10');
CREATE USER MAPPING FOR CURRENT_USER SERVER ch_bench OPTIONS (user :'ch_user', password :'ch_pass');

DROP SCHEMA IF EXISTS ch CASCADE;
CREATE SCHEMA ch;
IMPORT FOREIGN SCHEMA tpch_sf10 FROM SERVER ch_bench INTO ch;

CREATE SCHEMA IF NOT EXISTS pg;
SET search_path = pg;

DROP TABLE IF EXISTS REGION, NATION, PART, SUPPLIER, PARTSUPP, CUSTOMER, ORDERS, LINEITEM, LINEITEM_F64 CASCADE;

CREATE TABLE REGION
(
    R_REGIONKEY INTEGER  NOT NULL PRIMARY KEY,
    R_NAME      CHAR(25) NOT NULL,
    R_COMMENT   VARCHAR(152)
);

CREATE TABLE NATION
(
    N_NATIONKEY INTEGER  NOT NULL PRIMARY KEY,
    N_NAME      CHAR(25) NOT NULL,
    N_REGIONKEY INTEGER  NOT NULL,
    N_COMMENT   VARCHAR(152),
    FOREIGN KEY (N_REGIONKEY) REFERENCES REGION (R_REGIONKEY)
);

CREATE TABLE PART
(
    P_PARTKEY     INTEGER        NOT NULL PRIMARY KEY,
    P_NAME        VARCHAR(55)    NOT NULL,
    P_MFGR        CHAR(25)       NOT NULL,
    P_BRAND       CHAR(10)       NOT NULL,
    P_TYPE        VARCHAR(25)    NOT NULL,
    P_SIZE        INTEGER        NOT NULL,
    P_CONTAINER   CHAR(10)       NOT NULL,
    P_RETAILPRICE DECIMAL(15, 2) NOT NULL,
    P_COMMENT     VARCHAR(23)    NOT NULL
);
CREATE INDEX ON PART (P_NAME);

CREATE TABLE SUPPLIER
(
    S_SUPPKEY   INTEGER        NOT NULL PRIMARY KEY,
    S_NAME      CHAR(25)       NOT NULL,
    S_ADDRESS   VARCHAR(40)    NOT NULL,
    S_NATIONKEY INTEGER        NOT NULL,
    S_PHONE     CHAR(15)       NOT NULL,
    S_ACCTBAL   DECIMAL(15, 2) NOT NULL,
    S_COMMENT   VARCHAR(101)   NOT NULL,
    FOREIGN KEY (S_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
);
CREATE INDEX ON SUPPLIER (S_NAME);

CREATE TABLE PARTSUPP
(
    PS_PARTKEY    INTEGER        NOT NULL,
    PS_SUPPKEY    INTEGER        NOT NULL,
    PS_AVAILQTY   INTEGER        NOT NULL,
    PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
    PS_COMMENT    VARCHAR(199)   NOT NULL,
    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
    FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER (S_SUPPKEY),
    FOREIGN KEY (PS_PARTKEY) REFERENCES PART (P_PARTKEY)
);

CREATE TABLE CUSTOMER
(
    C_CUSTKEY    INTEGER        NOT NULL PRIMARY KEY,
    C_NAME       VARCHAR(25)    NOT NULL,
    C_ADDRESS    VARCHAR(40)    NOT NULL,
    C_NATIONKEY  INTEGER        NOT NULL,
    C_PHONE      CHAR(15)       NOT NULL,
    C_ACCTBAL    DECIMAL(15, 2) NOT NULL,
    C_MKTSEGMENT CHAR(10)       NOT NULL,
    C_COMMENT    VARCHAR(117)   NOT NULL,
    FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
);

CREATE TABLE ORDERS
(
    O_ORDERKEY      INTEGER        NOT NULL PRIMARY KEY,
    O_CUSTKEY       INTEGER        NOT NULL,
    O_ORDERSTATUS   CHAR(1)        NOT NULL,
    O_TOTALPRICE    DECIMAL(15, 2) NOT NULL,
    O_ORDERDATE     DATE           NOT NULL,
    O_ORDERPRIORITY CHAR(15)       NOT NULL,
    O_CLERK         CHAR(15)       NOT NULL,
    O_SHIPPRIORITY  INTEGER        NOT NULL,
    O_COMMENT       VARCHAR(79)    NOT NULL,
    FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY)
);
CREATE INDEX ON ORDERS (O_ORDERDATE);

CREATE TABLE LINEITEM
(
    L_ORDERKEY      INTEGER        NOT NULL,
    L_PARTKEY       INTEGER        NOT NULL,
    L_SUPPKEY       INTEGER        NOT NULL,
    L_LINENUMBER    INTEGER        NOT NULL,
    L_QUANTITY      NUMERIC(15, 2) NOT NULL,
    L_EXTENDEDPRICE NUMERIC(15, 2) NOT NULL,
    L_DISCOUNT      NUMERIC(15, 2) NOT NULL,
    L_TAX           NUMERIC(15, 2) NOT NULL,
    L_RETURNFLAG    CHAR(1)        NOT NULL,
    L_LINESTATUS    CHAR(1)        NOT NULL,
    L_SHIPDATE      DATE           NOT NULL,
    L_COMMITDATE    DATE           NOT NULL,
    L_RECEIPTDATE   DATE           NOT NULL,
    L_SHIPINSTRUCT  CHAR(25)       NOT NULL,
    L_SHIPMODE      CHAR(10)       NOT NULL,
    L_COMMENT       VARCHAR(44)    NOT NULL
);

-- Float64 lineitem variant: only the numeric columns become double precision;
-- integer keys stay INTEGER. Populated from LINEITEM below.
CREATE TABLE LINEITEM_F64 (
    l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL, l_linenumber INTEGER NOT NULL,
    l_quantity DOUBLE PRECISION NOT NULL, l_extendedprice DOUBLE PRECISION NOT NULL,
    l_discount DOUBLE PRECISION NOT NULL, l_tax DOUBLE PRECISION NOT NULL,
    l_returnflag CHAR(1) NOT NULL, l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL, l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
);

\set ECHO queries
-- Scaling factor 10 (zstd-compressed; the postgres OS user needs curl + zstd on PATH).
COPY region   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/region.tbl.zst   | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY nation   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/nation.tbl.zst   | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY part     FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/part.tbl.zst     | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY supplier FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/supplier.tbl.zst | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY partsupp FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/partsupp.tbl.zst | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY customer FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/customer.tbl.zst | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY orders   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/orders.tbl.zst   | zstd -dc' WITH (FORMAT csv, DELIMITER '|');
COPY lineitem FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/10/lineitem.tbl.zst | zstd -dc' WITH (FORMAT csv, DELIMITER '|');

-- Float64 lineitem variant: numeric -> float8 assignment cast.
INSERT INTO lineitem_f64 SELECT * FROM lineitem;

CREATE INDEX ON LINEITEM (L_PARTKEY, L_SUPPKEY);
CREATE INDEX ON LINEITEM (L_ORDERKEY);
CREATE INDEX ON LINEITEM (L_SHIPDATE);
ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY, L_LINENUMBER);
ANALYZE;

COMMIT;
