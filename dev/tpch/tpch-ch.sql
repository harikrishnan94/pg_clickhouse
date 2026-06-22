-- https://clickhouse.com/docs/getting-started/example-datasets/tpch
--
-- Isolated benchmark database: tpch_sf10 (scaling factor 10). Everything uses
-- IF NOT EXISTS / TRUNCATE so `make ch` is safe to re-run and never touches a
-- database it did not create.

CREATE DATABASE IF NOT EXISTS tpch_sf10;
USE tpch_sf10;

CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  Int32,
    n_name       String,
    n_regionkey  Int32,
    n_comment    String)
ORDER BY (n_nationkey);

CREATE TABLE IF NOT EXISTS region (
    r_regionkey  Int32,
    r_name       String,
    r_comment    String)
ORDER BY (r_regionkey);

CREATE TABLE IF NOT EXISTS part (
    p_partkey     Int32,
    p_name        String,
    p_mfgr        String,
    p_brand       String,
    p_type        String,
    p_size        Int32,
    p_container   String,
    p_retailprice Decimal(15,2),
    p_comment     String)
ORDER BY (p_partkey);

CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey     Int32,
    s_name        String,
    s_address     String,
    s_nationkey   Int32,
    s_phone       String,
    s_acctbal     Decimal(15,2),
    s_comment     String)
ORDER BY (s_suppkey);

CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey     Int32,
    ps_suppkey     Int32,
    ps_availqty    Int32,
    ps_supplycost  Decimal(15,2),
    ps_comment     String)
ORDER BY (ps_partkey, ps_suppkey);

CREATE TABLE IF NOT EXISTS customer (
    c_custkey     Int32,
    c_name        String,
    c_address     String,
    c_nationkey   Int32,
    c_phone       String,
    c_acctbal     Decimal(15,2),
    c_mktsegment  String,
    c_comment     String)
ORDER BY (c_custkey);

CREATE TABLE IF NOT EXISTS orders  (
    o_orderkey       Int32,
    o_custkey        Int32,
    o_orderstatus    String,
    o_totalprice     Decimal(15,2),
    o_orderdate      Date,
    o_orderpriority  String,
    o_clerk          String,
    o_shippriority   Int32,
    o_comment        String)
ORDER BY (o_orderkey);

CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey       Int32,
    l_partkey        Int32,
    l_suppkey        Int32,
    l_linenumber     Int32,
    l_quantity       Decimal(15,2),
    l_extendedprice  Decimal(15,2),
    l_discount       Decimal(15,2),
    l_tax            Decimal(15,2),
    l_returnflag     String,
    l_linestatus     String,
    l_shipdate       Date,
    l_commitdate     Date,
    l_receiptdate    Date,
    l_shipinstruct   String,
    l_shipmode       String,
    l_comment        String)
ORDER BY (l_orderkey, l_linenumber);

-- Float64 lineitem variant: only the Numeric/Decimal columns become Float64;
-- integer keys stay Int32. Used to compare exact-Decimal vs Float64 SHM offload.
CREATE TABLE IF NOT EXISTS lineitem_f64 (
    l_orderkey Int32, l_partkey Int32, l_suppkey Int32, l_linenumber Int32,
    l_quantity Float64, l_extendedprice Float64, l_discount Float64, l_tax Float64,
    l_returnflag String, l_linestatus String,
    l_shipdate Date, l_commitdate Date, l_receiptdate Date,
    l_shipinstruct String, l_shipmode String, l_comment String)
ORDER BY (l_orderkey, l_linenumber);

-- Scaling factor 10 (ships as .tbl.zst; s3() auto-detects zstd).
TRUNCATE TABLE nation;
TRUNCATE TABLE region;
TRUNCATE TABLE part;
TRUNCATE TABLE supplier;
TRUNCATE TABLE partsupp;
TRUNCATE TABLE customer;
TRUNCATE TABLE orders;
TRUNCATE TABLE lineitem;
TRUNCATE TABLE lineitem_f64;

INSERT INTO nation   SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/nation.tbl.zst',   NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO region   SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/region.tbl.zst',   NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO part     SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/part.tbl.zst',     NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO supplier SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/supplier.tbl.zst', NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO partsupp SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/partsupp.tbl.zst', NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO customer SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/customer.tbl.zst', NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO orders   SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/orders.tbl.zst',   NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
INSERT INTO lineitem SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/10/lineitem.tbl.zst', NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;

-- Float64 lineitem variant: derive from the loaded decimal lineitem (no second download).
INSERT INTO lineitem_f64
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
       toFloat64(l_quantity), toFloat64(l_extendedprice), toFloat64(l_discount), toFloat64(l_tax),
       l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
       l_shipinstruct, l_shipmode, l_comment
FROM lineitem;

/*

-- Scaling factor 1
INSERT INTO nation SELECT * FROM s3('https://clickhouse-datasets.s3.amazonaws.com/h/1/nation.tbl', NOSIGN, CSV) SETTINGS format_csv_delimiter = '|', input_format_defaults_for_omitted_fields = 1, input_format_csv_empty_as_default = 1;
-- ... (see git history for the full SF=1 / SF=100 blocks)

*/
