/*
drop view CHECK_WITH_PPTF_AGG_V;
drop view CHECK_WITH_PPTF_V;
drop table THINNING_LOG;
drop sequence THINNING_LOG_S;
drop type STALE_LIST_T; 
drop type STALE_T;
drop sequence STALES_S;
drop table STALES purge;
drop table QUOTES purge;
drop table TRANSACTIONS purge;
drop table POLLS_TRANSACTIONS purge;
drop table POLLS_DIRECTORY_STOCKS purge;
drop sequence POLLS_DIRECTORY_S;
drop table POLLS_DIRECTORY purge;
drop table REF_STOCKS purge;
drop sequence REF_STOCKS_S;
*/


--******************************************************************************

create sequence REF_STOCKS_S nocache;

create table REF_STOCKS (
      ID                    number not null
    , ANAME                 varchar2(64) not null
    , IS_ACTIVE             char (1) not null
    , CREATED_TSLTZ         timestamp (6) with local time zone
    , INACTIVATED_TSLTZ     timestamp (6) with local time zone
    , LAST_POLL_TSLTZ       timestamp with local time zone
    , LAST_POLL_RESULT      char (1)
    , CONTIGUOUS_M_COUNT    number
    , MAX_UT                number
    , constraint REF_STOCKS_PKIOT primary key (ID)
) organization index;

alter table REF_STOCKS add constraint REF_STOCKS_UK1 unique (ANAME);



create table POLLS_DIRECTORY (
      ID                number not null
    , POLL_START_TSLTZ  timestamp (6) with local time zone
    , DURATION_N        number
    , ARESULT           char (1)
    , ORAERRNUM         number
    , ORAERRM           varchar2(4000)
);

alter table POLLS_DIRECTORY add constraint POLLS_DIRECTORY_PK primary key (ID);

create sequence POLLS_DIRECTORY_S nocache;



create table POLLS_DIRECTORY_STOCKS (
      POLL_ID           number not null
    , STOCK_ID          number not null
    , ARCHDATE_TSLTZ    timestamp with local time zone
    , ARCHSIZE          number
);

alter table POLLS_DIRECTORY_STOCKS add constraint POLLS_DIRECTORY_STOCKS_FKP1
foreign key (POLL_ID) references POLLS_DIRECTORY (ID) on delete cascade;

alter table POLLS_DIRECTORY_STOCKS add constraint POLLS_DIRECTORY_STOCKS_FKP2
foreign key (STOCK_ID) references REF_STOCKS (ID) on delete cascade;

alter table POLLS_DIRECTORY_STOCKS add constraint POLLS_DIRECTORY_STOCKS_PKIOT primary key (POLL_ID, STOCK_ID) using index;

create index POLLS_DIRECTORY_STOCKS_IFKP2 on POLLS_DIRECTORY_STOCKS (STOCK_ID);



create table POLLS_TRANSACTIONS (
      STOCK_ID          number not null
    , POLL_TSLTZ        timestamp (6) with local time zone
    , ARESULT           char (1 byte) check (ARESULT in ('S', 'X', 'M', 'L'))
                                         -- Success, eXception, eMpty, Locked
    , FILESIZE          number
    , REQUESTED_UT      number
    , REQUEST_URL       varchar2 (1000)
    , RECEIVED_MIN_UT   number
    , RECEIVED_MAX_UT   number
    , POLL_DURATION_N   number
    , ALL_DURATION_N    number
    , CNT_LINES         number
    , ORAERRNUM         number
    , ORAERRMSG         varchar2(4000)
);

create index POLLS_TRANSACTIONS_IFKP on POLLS_TRANSACTIONS (STOCK_ID); 

alter table POLLS_TRANSACTIONS add constraint POLLS_TRANSACTIONS_FKP foreign key (STOCK_ID) references REF_STOCKS (ID) on delete cascade;



create table TRANSACTIONS (
      STOCK_ID    number not null
    , UT          number not null
    , SEQ_NUM     number not null
    , APRICE      number not null
    , AVOLUME     number not null
    , IS_LIVE     char (1 byte)
    , constraint TRANSACTIONS_PKIOT primary key (STOCK_ID, UT, SEQ_NUM)
) organization index compress 2;

alter table TRANSACTIONS add constraint TRANSACTIONS_FKP foreign key (STOCK_ID) references REF_STOCKS (ID) on delete cascade;



create table QUOTES (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT_PARENT     number not null
    , UT            number not null
    , AOPEN         number not null
    , AMIN          number not null
    , AMAX          number not null
    , ACLOSE        number not null
    , AVOLUME       number not null
    , ACOUNT        number not null
    , constraint QUOTES_PKIOT primary key (STRIPE_ID, STOCK_ID, UT_PARENT, UT)
) organization index compress 3;



create sequence STALES_S cache 1000000;

create table STALES (
      STRIPE_ID     number not null
    , STOCK_ID      number not null
    , UT            number not null
    , SEQ_NUM       number not null
    , constraint STALES_PKIOT primary key (STRIPE_ID, STOCK_ID, UT, SEQ_NUM)
) organization index;



create or replace trigger TRANSACTIONS_TAIUDT after insert or delete or update on TRANSACTIONS
begin
    dbms_alert.signal (PROCESS_P.g_alert_name, null);
end;

create or replace trigger TRANSACTIONS_TAIUDR after insert or delete or update on TRANSACTIONS for each row
begin
    if    inserting or updating then
        insert into STALES (STRIPE_ID, STOCK_ID, UT, SEQ_NUM) values (1, :new.STOCK_ID, :new.UT, STALES_S.nextval);
    elsif deleting  or updating then
        insert into STALES (STRIPE_ID, STOCK_ID, UT, SEQ_NUM) values (1, :old.STOCK_ID, :old.UT, STALES_S.nextval);
    end if;
end;

create or replace trigger QUOTES_TAIUDR after insert or delete or update on QUOTES for each row
begin
    if    inserting or updating then
        if :new.STRIPE_ID + 1 <= 18 then
            insert into STALES (STRIPE_ID, STOCK_ID, UT, SEQ_NUM) values (:new.STRIPE_ID + 1, :new.STOCK_ID, :new.UT_PARENT, STALES_S.nextval);
        end if;
    elsif deleting  or updating then
            insert into STALES (STRIPE_ID, STOCK_ID, UT, SEQ_NUM) values (:old.STRIPE_ID + 1, :old.STOCK_ID, :old.UT_PARENT, STALES_S.nextval);
    end if;
end;



create type STALE_T as object (STRIPE_ID number, STOCK_ID number, UT number);
create type STALE_LIST_T as table of STALE_T; 


create sequence THINNING_LOG_S;

create table THINNING_LOG (
      ID number
    , START_TSLTZ       timestamp with local time zone not null
    , CNT_STALES        number
    , CNT_STALES_DIST   number
    , CNT_INS           number
    , CNT_UPD           number
    , CNT_DEL           number
    , CNT_DEL_TAILS     number
    , DUR_LOCK          number
    , DUR_OPEN_C1       number
    , DUR_OPEN_C2       number
    , DUR_ALL           number);
    
alter table THINNING_LOG add constraint THINNING_LOG_PK primary key (ID);
    

--******************************************************************************

create view CHECK_WITH_PPTF_V as
select coalesce (a.STRIPE_ID, b.STRIPE_ID) as STRIPE_ID, coalesce (a.STOCK_ID, b.STOCK_ID) as STOCK_ID, coalesce (a.UT, b.UT) as UT
     , '      ' as SEP1
     , a.AOPEN as ORIG_AOPEN, a.AMIN as ORIG_AMIN, a.AMAX as ORIG_AMAX, a.ACLOSE as ORIG_ACLOSE, a.AVOLUME as ORIG_AVOLUME, a.ACOUNT as ORIG_ACOUNT 
     , '      ' as SEP2
     , b.AOPEN as PPTF_AOPEN, b.AMIN as PPTF_AMIN, b.AMAX as PPTF_AMAX, b.ACLOSE as PPTF_ACLOSE, b.AVOLUME as PPTF_AVOLUME, b.ACOUNT as PPTF_ACOUNT
     , '      ' as SEP3
     , case when nvl (a.AOPEN,   -1) = nvl (b.AOPEN, -1)
             and nvl (a.AMIN,    -1) = nvl (b.AMIN,    -1)
             and nvl (a.AMAX,    -1) = nvl (b.AMAX,    -1)
             and nvl (a.ACLOSE,  -1) = nvl (b.ACLOSE,  -1)
             and nvl (a.AVOLUME, -1) = nvl (b.AVOLUME, -1)
             and nvl (a.ACOUNT,  -1) = nvl (b.ACOUNT,  -1)
       then 0 else 1 end as NEQU
from QUOTES a
full outer join table (THINNING.THINNING_PPTF_P.F (cursor (select STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME from TRANSACTIONS))) b
on (a.STRIPE_ID = b.STRIPE_ID and a.STOCK_ID = b.STOCK_ID and a.UT = b.UT)
order by 1, 2, 3;


create view CHECK_WITH_PPTF_AGG_V as
select STRIPE_ID, NEQU, count (*) as CNT from CHECK_WITH_PPTF_V
group by cube (STRIPE_ID, NEQU);



