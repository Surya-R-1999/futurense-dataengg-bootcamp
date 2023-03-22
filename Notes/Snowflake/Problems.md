- Business Question: The Pricing Summary Report
Query provides a summary pricing report for 
all line items shipped as of a given date. The
date is within 60 - 120 days of the greatest 
ship date contained in the database. The query
lists totals for extended price, discounted 
extended price, discounted extended price plus
tax, average quantity, average extended price,
and average discount. These aggregates are 
grouped by RETURNFLAG and LINESTATUS, and 
listed in ascending order of RETURNFLAG and 
LINESTATUS. A count of the number of line items
in each group is included. 

    use schema snowflake_sample_data.tpch_sf1; 
    select sum(l_extendedprice) as extended_price, sum((1-l_discount)*l_extendedprice) as discounted_extended_price, 
    sum(((1-l_discount)*l_extendedprice) + l_tax) as discounted_exteded_price_with_tax, 
    avg(l_quantity) as avg_quantity, avg(l_extendedprice) as avg_extendedprice, 
    avg(l_discount) as avg_discount, count(*) as l_count
    from lineitem 
    where l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
    group by l_returnflag,l_linestatus 
    order by l_returnflag , l_linestatus;
