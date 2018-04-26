
--****** Command 9.1

merge
into REF_STOCKS dst
using (select distinct STOCK_NAME from TRANSACTIONS_RAW) src
on (dst.ANAME = src.STOCK_NAME)
when not matched then insert (ID, ANAME) values (REF_STOCKS_S.nextval, src.STOCK_NAME);

commit;

--****** Command 9.2

exec dbms_stats.set_table_prefs    ('THINNING', 'REF_STOCKS',       'METHOD_OPT', 'FOR ALL INDEXED COLUMNS SIZE AUTO')
exec dbms_stats.set_table_prefs    ('THINNING', 'TRANSACTIONS_RAW', 'METHOD_OPT', 'FOR ALL COLUMNS SIZE')
exec dbms_stats.gather_table_stats ('THINNING', 'REF_STOCKS')
exec dbms_stats.gather_table_stats ('THINNING', 'TRANSACTIONS_RAW')

exec dbms_stats.unlock_table_stats ('THINNING', 'TRANSACTIONS')
exec dbms_stats.delete_table_stats ('THINNING', 'TRANSACTIONS')
exec dbms_stats.lock_table_stats   ('THINNING', 'TRANSACTIONS')


--****** Command 9.3

insert into TRANSACTIONS (STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME, TRANSACTION_NUM)
with
  T1 as (select b.ID as STOCK_ID, UT
              , row_number ()   over (partition by b.ID, a.UT order by a.ID) - 1 as SEQ_NUM
              , APRICE
              , AVOLUME           
              , row_number ()   over (partition by b.ID order by a.UT, a.ID) - 1 as TRANS_NUM_IN_STOCK
              , avg (APRICE)    over (partition by b.ID order by a.UT, a.ID rows between 100 preceding and 100 following) as APRICE_EXP_VAL
              , stddev (APRICE) over (partition by b.ID order by a.UT, a.ID rows between 100 preceding and 100 following) as APRICE_STD_DEV
      from TRANSACTIONS_RAW a
      join REF_STOCKS b on (a.STOCK_NAME = b.ANAME)
      )
select STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME
     , row_number () over (order by TRANS_NUM_IN_STOCK, STOCK_ID) - 1 as TRANSACTION_NUM
from T1
where APRICE between APRICE_EXP_VAL - 2 * APRICE_STD_DEV and APRICE_EXP_VAL + 2 * APRICE_STD_DEV
order by 1, 2, 3;
 
commit;

--****** Command 9.4

-- This expensive index needed only for uniform querying source treansactions to send to input thinning functions
--create unique index TRANSACTIONS_I1 on TRANSACTIONS (TRANSACTION_NUM, STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME);




