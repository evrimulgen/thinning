drop view CHECK_QUOTES_STRONG_V;
drop view CHECK_QUOTES_SIMPLE_V;
drop view TIMES_V;

drop table THINNING_LOG purge;
drop table QUOTES_SIMP purge;
drop table QUOTES_CALC purge;
drop table QUOTES_UDAF purge;
drop table QUOTES_PPTF purge;
drop table QUOTES_MODE purge;

drop table TRANSACTIONS purge;
drop table REF_STOCKS purge;
drop sequence REF_STOCKS_S;
drop table TRANSACTIONS_RAW purge;
drop table COMMIT_PERIODICAL_LOG;

--******************************************************************************


create table COMMIT_PERIODICAL_LOG (
      TSLTZ     timestamp with local time zone
    , TAG varchar2(256)
);

create index COMMIT_PERIODICAL_LOG_I1 on COMMIT_PERIODICAL_LOG(TSLTZ);


create table TRANSACTIONS_RAW (
      ID            number  not null
    , STOCK_NAME    varchar2 (32)
    , UT            number  not null
    , APRICE        number  not null
    , AVOLUME       number not null
    , STOCK_ID      number
    , SEQ_NUM       number
    , DUMMY_2       number
)
tablespace DATA_064M;

alter table TRANSACTIONS_RAW add constraint TRANSACTIONS_RAW_PK primary key (ID) using index unusable;

grant select on TRANSACTIONS_RAW to public;


create sequence REF_STOCKS_S nocache;

create table REF_STOCKS (
      ID        number
    , ANAME     varchar2(32) not null
    , constraint REF_STOCKS_PKIOT primary key (ID)
) organization index;

alter table REF_STOCKS add constraint REF_STOCK_UK1 unique (ANAME);


create table TRANSACTIONS (
      STOCK_ID          number not null
    , UT                number not null
    , SEQ_NUM           number not null
    , APRICE            number not null
    , AVOLUME           number not null
    , TRANSACTION_NUM   number
--    , constraint TRANSACTIONS_PKIOT primary key (STOCK_ID, UT, SEQ_NUM)
)
--organization index pctthreshold 50 compress 2
parallel 4 nologging;

alter table TRANSACTIONS add constraint TRANSACTIONS_FKP
foreign key (STOCK_ID) references THINNING_CORE.REF_STOCKS (ID);

--******************************************************************************

create table QUOTES_SIMP (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
);


create table QUOTES_CALC (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
);
create bitmap index QUOTES_CALC_IB1 on QUOTES_CALC (STRIPE_ID);


create table QUOTES_UDAF (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
);

create bitmap index QUOTES_UDAF_IB1 on QUOTES_UDAF (STRIPE_ID);


create table QUOTES_PPTF (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
);

create table QUOTES_MODE (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
);

create table THINNING_LOG (
      TEST_CASE     varchar2 (128) not null
    , ROW_COUNT     number      not null
    , ALG_NAME      varchar(32) not null
    , DURATION_IDS  interval day (0) to second (6)
);

create index THINNING_LOG_I1 on THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME);


create view CHECK_QUOTES_SIMPLE_V as 
select 'SIMP' as ALG_NAME, count (*) as CNT from QUOTES_SIMP
union all
select 'CALC'            , count (*)        from QUOTES_CALC
union all
select 'UDAF'            , count (*)        from QUOTES_UDAF
union all
select 'PPTF'            , count (*)        from QUOTES_PPTF
union all
select 'MODE'            , count (*)        from QUOTES_MODE
order by 1;


create view CHECK_QUOTES_STRONG_V as
with
  T1 as (select 'SIMP' as ALG_NAME, STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT from QUOTES_SIMP
         union all
         select 'CALC' as ALG_NAME, STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT from QUOTES_CALC
         union all
         select 'UDAF' as ALG_NAME, STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT from QUOTES_UDAF
         union all
         select 'PPTF' as ALG_NAME, STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT from QUOTES_PPTF
         union all
         select 'MODE' as ALG_NAME, STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT from QUOTES_MODE)
, T2 as (select ALG_NAME
              , 3 * row_number () over (partition by ALG_NAME order by STRIPE_ID, STOCK_ID, UT) - 1
              + 5 * STRIPE_ID + 7 * STOCK_ID + 11 * UT + 13 * AOPEN + 17 * AMIN + 19 * AMAX + 23 * ACLOSE + 29 * AVOLUME + 31 * ACOUNT as PSEUDO_HASH_ROW
         from T1)
select ALG_NAME, to_char (sum (PSEUDO_HASH_ROW)) as PSEUDO_HASH
from T2
group by ALG_NAME
order by 1;



create force view TIMES_V as
with
  T1 as (select TEST_CASE, ROW_COUNT, ALG_NAME, round (avg (TLS_P.INTERVALDS2NUMBER (DURATION_IDS)), 10) as DURATION_S from THINNING_LOG group by TEST_CASE, ROW_COUNT, ALG_NAME)
, T2 as (select * from (select * from T1 where DURATION_S > 0)
         pivot (avg (DURATION_S) for (ALG_NAME) in ('SIMP' as SIMP, 'CALC' as CALC, 'UDAF' as UDAF, 'PPTF' as PPTF, 'ONES' as ONES)))
select TEST_CASE, to_char (ROW_COUNT, '999G999G999G990') as ROW_COUNT
     , rtrim (to_char (SIMP, '9999990.00')) as SIMP_D
     , rtrim (to_char (CALC, '9999990.00')) as CALC_D
     , rtrim (to_char (UDAF, '9999990.00')) as UDAF_D
     , rtrim (to_char (PPTF, '9999990.00')) as PPTF_D
     , rtrim (to_char (ONES, '9999990.00')) as ONES_D
     , '-' S
     , rtrim (to_char (ROW_COUNT / SIMP, '9999990.00')) as SIMP_TRPS
     , rtrim (to_char (ROW_COUNT / CALC, '9999990.00')) as CALC_TRPS
     , rtrim (to_char (ROW_COUNT / UDAF, '9999990.00')) as UDAF_TRPS
     , rtrim (to_char (ROW_COUNT / PPTF, '9999990.00')) as PPTF_TRPS
     , rtrim (to_char (ROW_COUNT / ONES, '9999990.00')) as ONES_TRPS
from T2
order by 1, 2;
