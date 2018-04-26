drop procedure THINNING_ALL_T;
drop procedure THINNING_ALL_SIMP_T;
drop procedure THINNING_ALL_CALC_T;
drop procedure THINNING_ALL_UDAF_T;
drop procedure THINNING_ALL_PPTF_T;
drop procedure THINNING_ALL_MODE_T;
drop package THINNING_PPTF_P;
drop function THINNING_UDAF;
drop type THINNING_UDAF_IMPL_T;
drop type QUOTES_T;
drop type QUOTE_T;
drop package TLS_P;

--******************************************************************************
--****** CREATING
--******************************************************************************

create package TLS_P is

function DATE2UT (p_D date) return number deterministic;
function DATESTR2UT (p_S varchar2) return number deterministic;
function UT2DATE (p_UT number) return date deterministic;
function UT2DATESTR (p_UT number) return varchar2 deterministic;
procedure COMMIT_PERIODICAL (p_period_seconds integer default 60, p_tag varchar2 default null);
function INTERVALDS2NUMBER (p interval day to second) return number deterministic;
function TRUNC_UT (p_UT number, p_StripeTypeId number) return number deterministic result_cache parallel_enable;

end;
/

create package body TLS_P is

g_commit_periodical_last timestamp with local time zone;

function DATE2UT (p_D date) return number deterministic is
begin
  return (p_D - to_date ('01.01.1970 00:00:00', 'DD.MM.YYYY HH24:MI:SS')) * 86400;
end;    

function DATESTR2UT (p_S varchar2) return number deterministic is
begin
  return DATE2UT (to_date (p_S, 'YYYY.MM.DD HH24:MI:SS'));
end;    

function UT2DATE (p_UT number) return date deterministic is
begin
  return to_date ('01.01.1970 00:00:00', 'DD.MM.YYYY HH24:MI:SS') + p_UT / 86400;
end;    

function UT2DATESTR (p_UT number) return varchar2 deterministic is
begin
    return to_char (UT2DATE (p_UT), 'YYYY.MM.DD HH24:MI:SS');
end;


procedure COMMIT_PERIODICAL (p_period_seconds integer default 60, p_tag varchar2 default null) is
begin

    if g_commit_periodical_last is null
    then g_commit_periodical_last := systimestamp;
    elsif systimestamp - g_commit_periodical_last >= numtodsinterval (p_period_seconds, 'SECOND')
    then
        insert into COMMIT_PERIODICAL_LOG (TAG) values (p_tag); 
        commit;
        g_commit_periodical_last := systimestamp;
    end if;

end;


function INTERVALDS2NUMBER (p interval day to second) return number deterministic is
begin
    return extract (second from p)
         + extract (minute from p) * 60
         + extract (hour   from p) * 60 * 60
         + extract (day    from p) * 60 * 60 * 24;
end;

function TRUNC_UT (p_UT number, p_StripeTypeId number) return number deterministic result_cache parallel_enable is
    ret_value   number;
begin
    case p_StripeTypeId
    when 1  then ret_value := trunc (p_UT / 1) * 1;
    when 2  then ret_value := trunc (p_UT / 5) * 5;
    when 3  then ret_value := trunc (p_UT / 15) * 15;
    when 4  then ret_value := trunc (p_UT / 30) * 30;
    when 5  then ret_value :=  trunc (p_UT / 60) * 60;
    when 6  then ret_value :=  trunc (p_UT / 300) * 300;
    when 7  then ret_value :=  trunc (p_UT / 900) * 900;
    when 8  then ret_value :=  trunc (p_UT / 1800) * 1800;
    when 9  then ret_value :=  trunc (p_UT / 3600) * 3600;
    when 10 then ret_value :=  trunc (p_UT / ( 4 * 3600)) * ( 4 * 3600);
    when 11 then ret_value :=  trunc (p_UT / (12 * 3600)) * (12 * 3600);
    when 12 then ret_value :=  trunc (p_UT / (24 * 3600)) * (24 * 3600);
    when 13 then ret_value :=  DATE2UT (trunc (UT2DATE(p_UT), 'Month'));
    when 14 then ret_value :=  DATE2UT (trunc (UT2DATE(p_UT), 'Q'));
    when 15 then ret_value :=  DATE2UT (trunc (UT2DATE(p_UT), 'YYYY'));
    when 16 then ret_value :=  DATE2UT (to_date ('01.01.'||ltrim(to_char(trunc ((extract (year from UT2DATE (p_UT)) - 1970) / 10) * 10 + 1970, '9999'))||' 00:00:00', 'DD.MM.YYYY HH24:MI:SS'));
    when 17 then ret_value :=  DATE2UT (to_date ('01.01.'||ltrim(to_char(trunc ((extract (year from UT2DATE (p_UT)) - 1970) / 50) * 50 + 1970, '9999'))||' 00:00:00', 'DD.MM.YYYY HH24:MI:SS'));
    when 18 then ret_value :=  0;
    when 19 then ret_value :=  0;
    when 20 then ret_value :=  0;
    else raise_application_error (-20001, 'Unknown stripe type ID = ' || p_StripeTypeId);
    end case;

    return round (ret_value);

end;  

end;
/

grant execute on TLS_P to public;


--******** SIMP ****************************************************************

create procedure THINNING_ALL_SIMP_T (l_max_transaction_num number, p_case_name varchar2) is
l_op_start_tsltz timestamp with local time zone;
begin

    rollback;

    execute immediate 'truncate table QUOTES_SIMP';

    l_op_start_tsltz := systimestamp;
    
    insert into QUOTES_SIMP
    with 
      T1 as (select --Z+ index_asc (a TRANSACTIONS_PKIOT)
                    STOCK_ID
                  , UT
                  , avg (APRICE)  keep (dense_rank first order by SEQ_NUM)               as AOPEN
                  , min (APRICE)                                                         as AMIN
                  , max (APRICE)                                                         as AMAX
                  , avg (APRICE)  keep (dense_rank last  order by SEQ_NUM)               as ACLOSE
                  , sum (AVOLUME)                                                        as AVOLUME
                  , count (*)                                                            as ACOUNT
             from TRANSACTIONS a
             where TRANSACTION_NUM <= l_max_transaction_num
--             where rownum <= l_max_transaction_num
             group by a.STOCK_ID, a.UT)
      , T2 as (select rownum as STRIPE_ID from dual connect by level <= 18)
    select
           b.STRIPE_ID
         , a.STOCK_ID
         , THINNING_CORE.TLS_P.TRUNC_UT (a.UT, b.STRIPE_ID)                     as UT
         , avg (AOPEN)   keep (dense_rank first order by UT)                    as AOPEN
         , min (AMIN)                                                           as AMIN
         , max (AMAX)                                                           as AMAX
         , avg (ACLOSE)  keep (dense_rank last  order by UT)                    as ACLOSE
         , sum (AVOLUME)                                                        as AVOLUME
         , sum (ACOUNT)                                                         as ACOUNT
    from T1 a, T2 b
    group by b.STRIPE_ID, a.STOCK_ID, THINNING_CORE.TLS_P.TRUNC_UT (a.UT, b.STRIPE_ID);

    insert into THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME, DURATION_IDS) values (p_case_name, l_max_transaction_num, 'SIMP', systimestamp - l_op_start_tsltz);
  
    commit;    
   
end;
/

--******** CALC ****************************************************************

create procedure THINNING_ALL_CALC_T (l_max_transaction_num number, p_case_name varchar2) is
l_op_start_tsltz timestamp with local time zone;
begin

    rollback;

    execute immediate 'truncate table QUOTES_CALC';

    l_op_start_tsltz := systimestamp;

    insert into QUOTES_CALC
    select --Z+ index_asc (a TRANSACTIONS_PKIOT)
           1 as STRIPE_ID
         , STOCK_ID
         , UT
         , avg (APRICE)  keep (dense_rank first order by SEQ_NUM)
         , min (APRICE)                                              
         , max (APRICE)                                              
         , avg (APRICE)  keep (dense_rank last  order by SEQ_NUM)
         , sum (AVOLUME)                                             
         , count (*)                                                 
    from TRANSACTIONS a
    where TRANSACTION_NUM <= l_max_transaction_num
--    where rownum <= l_max_transaction_num
    group by a.STOCK_ID, a.UT;
   
    for i in 1..17
    loop

        insert into QUOTES_CALC
        select --+ index (a QUOTES_CALC_IB1) 
               STRIPE_ID + 1                                    
             , STOCK_ID
             , THINNING_CORE.TLS_P.TRUNC_UT (UT, i + 1)
             , avg (AOPEN)   keep (dense_rank first order by UT)
             , min (AMIN)                                       
             , max (AMAX)                                       
             , avg (ACLOSE)  keep (dense_rank last  order by UT)
             , sum (AVOLUME)                                    
             , sum (ACOUNT)                                     
        from QUOTES_CALC
        where STRIPE_ID = i
        group by STRIPE_ID, STOCK_ID, THINNING_CORE.TLS_P.TRUNC_UT (UT, i + 1);

    end loop;
   
    insert into THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME, DURATION_IDS) values (p_case_name, l_max_transaction_num, 'CALC', systimestamp - l_op_start_tsltz);

    commit;
   
end;
/

--******** UDAF ****************************************************************

create type QUOTE_T as object (STRIPE_ID number, STOCK_ID number, UT number
                                        , AOPEN number, AMIN number, AMAX number, ACLOSE number
                                        , AVOLUME number, ACOUNT number);
/

create type QUOTES_T as table of QUOTE_T;
/

create type THINNING_UDAF_IMPL_T as object
(
    v_stripe_id             number,
    v_stock_id              number,
    v_ut_parent             number,

    v_ut_min                number,
    v_ut_max                number,

    v_price_ut_min          number,
    v_price_ut_max          number,
    v_price_min             number,
    v_price_max             number,

    v_volume                number,
    v_count                 number,
    static function ODCIAggregateInitialize   (sctx in out THINNING_UDAF_IMPL_T) return number,
    member function ODCIAggregateIterate      (self in out THINNING_UDAF_IMPL_T, value in QUOTE_T) return number,
    member function ODCIAggregateMerge        (self in out THINNING_UDAF_IMPL_T, ctx2 in THINNING_UDAF_IMPL_T) return number,
    member function ODCIAggregateTerminate    (self in THINNING_UDAF_IMPL_T, returnValue out QUOTE_T, flags in number) return number
);
/

create type body THINNING_UDAF_IMPL_T is
 
static function ODCIAggregateInitialize (sctx in out THINNING_UDAF_IMPL_T) return number is
begin
    sctx := THINNING_UDAF_IMPL_T (null, null, null
                                , null, null
                                , null, null, null, null
                                , 0, 0);
    return ODCIConst.Success;
end;

member function ODCIAggregateIterate (self in out THINNING_UDAF_IMPL_T, value in QUOTE_T) return number is
begin
    
    if self.v_stripe_id is null
    then self.v_stripe_id := value.STRIPE_ID;
    else if self.v_stripe_id <> value.STRIPE_ID
         then raise_application_error (-20001, 'All quotes must belongs to one stripe type');
         end if;
    end if;

    if self.v_stock_id is null
    then self.v_stock_id := value.STOCK_ID;
    else if self.v_stock_id <> value.STOCK_ID
         then raise_application_error (-20001, 'All quotes must belongs to one stock');
         end if;
    end if;

    if self.v_ut_parent is null
    then self.v_ut_parent := THINNING_CORE.TLS_P.TRUNC_UT (value.UT, value.STRIPE_ID + 1);
    else if self.v_ut_parent <> THINNING_CORE.TLS_P.TRUNC_UT (value.UT, value.STRIPE_ID + 1)
         then raise_application_error (-20001, 'All quotes must belongs to one parent stripe. Past='||self.v_ut_parent||', New='||THINNING_CORE.TLS_P.TRUNC_UT (value.UT, value.STRIPE_ID + 1));
         end if;
    end if;
    
    if value.UT < self.v_ut_min or self.v_ut_min is null then self.v_ut_min := value.UT; self.v_price_ut_min := value.AOPEN;  end if; 
    if value.UT > self.v_ut_max or self.v_ut_max is null then self.v_ut_max := value.UT; self.v_price_ut_max := value.ACLOSE; end if;
  
    if value.AMIN < self.v_price_min or self.v_price_min is null then self.v_price_min := value.AMIN; end if; 
    if value.AMAX > self.v_price_max or self.v_price_max is null then self.v_price_max := value.AMAX; end if;

    if self.v_volume is null then self.v_volume := value.AVOLUME; else self.v_volume := self.v_volume + value.AVOLUME; end if;

    self.v_count := self.v_count + value.ACOUNT;

    return ODCIConst.Success;
end;

member function ODCIAggregateMerge(self in out THINNING_UDAF_IMPL_T, ctx2 in THINNING_UDAF_IMPL_T) return number is
begin

    if ctx2.v_ut_min < self.v_ut_min or self.v_ut_min is null then self.v_ut_min := ctx2.v_ut_min; self.v_price_ut_min := ctx2.v_price_ut_min;  end if; 
    if ctx2.v_ut_max > self.v_ut_max or self.v_ut_max is null then self.v_ut_max := ctx2.v_ut_max; self.v_price_ut_max := ctx2.v_price_ut_max;  end if;
  
    if ctx2.v_price_min < self.v_price_min or self.v_price_min is null then self.v_price_min := ctx2.v_price_min; end if; 
    if ctx2.v_price_max > self.v_price_max or self.v_price_max is null then self.v_price_max := ctx2.v_price_max; end if;

    if self.v_volume is null then self.v_volume := ctx2.v_volume; else self.v_volume := self.v_volume + ctx2.v_volume; end if;
  
    return ODCIConst.Success;
end;


member function ODCIAggregateTerminate(self in THINNING_UDAF_IMPL_T, returnValue out QUOTE_T, flags in number) return number is
begin
    returnValue := QUOTE_T (self.v_stripe_id + 1
                          , self.v_stock_id
                          , v_ut_parent
                          , self.v_price_ut_min
                          , self.v_price_min
                          , self.v_price_max
                          , self.v_price_ut_max
                          , self.v_volume
                          , self.v_count
                        );
    return ODCIConst.Success;
end;

end;
/

create function THINNING_UDAF (input QUOTE_T) return QUOTE_T parallel_enable aggregate using THINNING_UDAF_IMPL_T;
/

create procedure THINNING_ALL_UDAF_T (l_max_transaction_num number, p_case_name varchar2) is
l_op_start_tsltz timestamp with local time zone;
begin

    rollback;

    execute immediate 'truncate table QUOTES_UDAF';

    l_op_start_tsltz := systimestamp;

    insert into QUOTES_UDAF
    select --Z+ index_asc (a TRANSACTIONS_PKIOT)
           1 as STRIPE_ID
         , a.STOCK_ID
         , a.UT
         , avg (APRICE)  keep (dense_rank first order by a.UT, a.SEQ_NUM)  as AOPEN
         , min (APRICE)                                                    as AMIN
         , max (APRICE)                                                    as AMAX
         , avg (APRICE)  keep (dense_rank last order by a.UT, a.SEQ_NUM)   as ACLOSE
         , sum (AVOLUME)                                                   as AVOLUME
         , count (*)                                                       as ACOUNT
    from TRANSACTIONS a
    where TRANSACTION_NUM <= l_max_transaction_num
--    where rownum <= l_max_transaction_num
    group by a.STOCK_ID, a.UT;
   
    for i in 1..17
    loop
        insert into QUOTES_UDAF
        with
            T1 as (select THINNING_UDAF (QUOTE_T (STRIPE_ID, STOCK_ID, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT)) as Q
                   from QUOTES_UDAF
                   where STRIPE_ID = i
                   group by STOCK_ID, THINNING_CORE.TLS_P.TRUNC_UT (UT, i + 1))
        select a.Q.STRIPE_ID, a.Q.STOCK_ID, a.Q.UT, a.Q.AOPEN, a.Q.AMIN, a.Q.AMAX, a.Q.ACLOSE, a.Q.AVOLUME, a.Q.ACOUNT
        from T1 a;
    end loop;
   
    insert into THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME, DURATION_IDS) values (p_case_name, l_max_transaction_num, 'UDAF', systimestamp - l_op_start_tsltz);

    commit;
  
end;
/

--******** PPTF ****************************************************************

create package THINNING_PPTF_P is

type TRANSACTION_RECORD_T is record (STOCK_ID number, UT number, SEQ_NUM number, APRICE number, AVOLUME number);
type QUOTE_E_T is record (STRIPE_ID number, STOCK_ID number, UT number, AOPEN number, AMIN number, AMAX number, ACLOSE number, AVOLUME number, ACOUNT number);


type CUR_RECORD_T is ref cursor return TRANSACTION_RECORD_T;
type QUOTES_E_T is table of QUOTE_E_T;

function F (p_cursor CUR_RECORD_T) return QUOTES_E_T
pipelined            order p_cursor by      (STOCK_ID, UT, SEQ_NUM)
parallel_enable (partition p_cursor by hash (STOCK_ID)   )
;

end;
/


create package body THINNING_PPTF_P is

function F (p_cursor CUR_RECORD_T) return QUOTES_E_T
pipelined            order p_cursor by      (STOCK_ID, UT, SEQ_NUM)
parallel_enable (partition p_cursor by hash (STOCK_ID)   )
is

    QT QUOTES_E_T := QUOTES_E_T() ;

    rec TRANSACTION_RECORD_T;
    rec_prev TRANSACTION_RECORD_T;
    
    type ut_T is table of number index by pls_integer;
    ut number;
begin

    QT.extend(18);

    loop
        fetch p_cursor into rec;
        exit when p_cursor%notfound;
   
        if rec_prev.STOCK_ID = rec.STOCK_ID
        then
            if    (rec.STOCK_ID = rec_prev.STOCK_ID and rec.UT < rec_prev.UT)
               or (rec.STOCK_ID = rec_prev.STOCK_ID and rec.UT = rec_prev.UT and rec.SEQ_NUM < rec_prev.SEQ_NUM)
            then raise_application_error (-20010, 'Rowset must be ordered, ('||rec_prev.STOCK_ID||','||rec_prev.UT||','||rec_prev.SEQ_NUM||') > ('||rec.STOCK_ID||','||rec.UT||','||rec.SEQ_NUM||')');
            end if;
        end if;


        if rec.STOCK_ID <> rec_prev.STOCK_ID or rec_prev.STOCK_ID is null
        then
            -- finalize begin
            for j in 1 .. 18
            loop
                if QT(j).UT is not null
                then
                    pipe row (QT(j));
                    QT(j) := null;
                end if;
            end loop;
            -- finalize end
        end if; 


        for i in reverse 1..18
        loop
            ut := THINNING_CORE.TLS_P.TRUNC_UT (rec.UT, i);
            
            if QT(i).UT <> ut
            then
                for j in 1..i
                loop
                    pipe row (QT(j));
                    QT(j) := null;
                end loop;
            end if;           
            
            if QT(i).UT is null
            then
                 QT(i).STRIPE_ID := i;
                 QT(i).STOCK_ID := rec.STOCK_ID;
                 QT(i).UT := ut;
                 QT(i).AOPEN := rec.APRICE;
            end if;

            if rec.APRICE < QT(i).AMIN or QT(i).AMIN is null then QT(i).AMIN := rec.APRICE; end if; 
            if rec.APRICE > QT(i).AMAX or QT(i).AMAX is null then QT(i).AMAX := rec.APRICE; end if;
            QT(i).AVOLUME := nvl (QT(i).AVOLUME, 0) + rec.AVOLUME;
            QT(i).ACOUNT := nvl (QT(i).ACOUNT, 0) + 1;
            QT(i).ACLOSE := rec.APRICE;     

        end loop;
     
        rec_prev := rec;
    end loop;

    -- finalize begin
    for j in 1 .. 18
    loop
        if QT(j).UT is not null
        then
            pipe row (QT(j));
        end if;
    end loop;
    -- finalize end

exception
        when no_data_needed then null;
end;

end;
/

grant execute on THINNING_PPTF_P to public;

create procedure THINNING_ALL_PPTF_T (l_max_transaction_num number, p_case_name varchar2) is
l_op_start_tsltz timestamp with local time zone;
begin

    rollback;

    execute immediate 'truncate table QUOTES_PPTF';

    
    l_op_start_tsltz := systimestamp;
    
    insert into QUOTES_PPTF
    select * from table (THINNING_PPTF_P.F (cursor (select --Z+ index_asc(a TRANSACTIONS_PKIOT)
                                                           STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME from TRANSACTIONS a
                                                    where TRANSACTION_NUM <= l_max_transaction_num
                                                    --where rownum <= l_max_transaction_num
                                                    )));
    
    insert into THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME, DURATION_IDS) values (p_case_name, l_max_transaction_num, 'PPTF', systimestamp - l_op_start_tsltz);

   
    commit;    
   
end;
/

--******** MODE ****************************************************************

create procedure THINNING_ALL_MODE_T (l_max_transaction_num number, p_case_name varchar2) is
l_op_start_tsltz timestamp with local time zone;
begin

    rollback;

    execute immediate 'truncate table QUOTES_MODE';

    
    l_op_start_tsltz := systimestamp;
    
    insert into QUOTES_MODE
    with
      SOURCETRANS
         as (select --Z+ index_asc(a TRANSACTIONS_PKIOT)
                    1 as STRIPE_ID, STOCK_ID, UT
                  , avg (APRICE)  keep (dense_rank first order by SEQ_NUM) as AOPEN
                  , min (APRICE)                                           as AMIN
                  , max (APRICE)                                           as AMAX
                  , avg (APRICE)  keep (dense_rank last  order by SEQ_NUM) as ACLOSE
                  , sum (AVOLUME)                                          as AVOLUME
                  , count (*)                                              as ACOUNT
             from TRANSACTIONS a
             where TRANSACTION_NUM <= l_max_transaction_num
--             where rownum <= l_max_transaction_num
             group by STOCK_ID, UT)
    , REFMOD_T1 (STRIPE_ID, STOCK_ID, PARENT_UT, UT)
        as (select 1, STOCK_ID, TLS_P.TRUNC_UT (UT, 2), UT
            from SOURCETRANS
            union all
            select STRIPE_ID + 1, STOCK_ID, TLS_P.TRUNC_UT (PARENT_UT, STRIPE_ID + 2) as PARENT_UT, PARENT_UT
            from REFMOD_T1
            where STRIPE_ID <= 17)
    , REFMOD_T2 as (select distinct * from REFMOD_T1 order by 1, 2, 3)
    , REFMOD_T3
        as (select --t+ use_merge (tab tabop tabcl)
                   tab.STRIPE_ID, tab.STOCK_ID, tab.PARENT_UT, tab.UT
                 , min (tabop.UT) as UT_OPEN, max (tabcl.UT) as UT_CLOSE
            from REFMOD_T2 tab, REFMOD_T2 tabop, REFMOD_T2 tabcl
            where tab.STRIPE_ID = tabop.STRIPE_ID(+) + 1 and tab.STOCK_ID = tabop.STOCK_ID(+) and tab.UT = tabop.PARENT_UT(+) 
              and tab.STRIPE_ID = tabcl.STRIPE_ID(+) + 1 and tab.STOCK_ID = tabcl.STOCK_ID(+) and tab.UT = tabcl.PARENT_UT(+)
            group by tab.STRIPE_ID, tab.STOCK_ID, tab.PARENT_UT, tab.UT)
    , REFMOD_FIN
        as (select STRIPE_ID, STOCK_ID, PARENT_UT, UT, UT_OPEN, UT_CLOSE
                 , nvl (lead (UT) over (partition by STRIPE_ID, STOCK_ID order by UT), 9999999999) as NEXT_STRIPE_UT
            from REFMOD_T3)
    , MAIN_TAB
        as (select a.STRIPE_ID, a.STOCK_ID, a.PARENT_UT, a.UT, b.AOPEN, b.AMIN, b.AMAX, b.ACLOSE, b.AVOLUME, b.ACOUNT, a.UT_OPEN, a.UT_CLOSE--, a.UT as UT_DUMMY
            from REFMOD_FIN a, SOURCETRANS b
            where a.STRIPE_ID = b.STRIPE_ID (+) and a.STOCK_ID = b.STOCK_ID (+) and a.UT = b.UT (+))
    select --+ parallel(4)
           STRIPE_ID, STOCK_ID, /*PARENT_UT,*/ UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT 
    from MAIN_TAB
    model
    reference st on (select * from REFMOD_FIN) dimension by (STRIPE_ID, STOCK_ID, UT) measures (/*PARENT_UT,*/ NEXT_STRIPE_UT, UT_OPEN, UT_CLOSE)
    main MM partition by (STOCK_ID) dimension by (STRIPE_ID, PARENT_UT, UT) measures (AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT/*, UT_DUMMY*/)
    rules iterate (18) (
      AOPEN   [iteration_number + 2, any, any] = AOPEN        [cv (STRIPE_ID) - 1, cv (UT), st.UT_OPEN [cv (STRIPE_ID), cv (STOCK_ID), cv (UT)]]                                    
    , ACLOSE  [iteration_number + 2, any, any] = ACLOSE       [cv (STRIPE_ID) - 1, cv (UT), st.UT_CLOSE[cv (STRIPE_ID), cv (STOCK_ID), cv (UT)]]                                    
    , AMIN    [iteration_number + 2, any, any] = min (AMIN)   [cv (STRIPE_ID) - 1, cv (UT), any]                                    
    , AMAX    [iteration_number + 2, any, any] = max (AMAX)   [cv (STRIPE_ID) - 1, cv (UT), any]                                    
    , AVOLUME [iteration_number + 2, any, any] = sum (AVOLUME)[cv (STRIPE_ID) - 1, cv (UT), any]                                    
    , ACOUNT  [iteration_number + 2, any, any] = sum (ACOUNT) [cv (STRIPE_ID) - 1, cv (UT), any]                                    
    )
--    order by 1, 2, 3, 4
;

    insert into THINNING_LOG (TEST_CASE, ROW_COUNT, ALG_NAME, DURATION_IDS) values (p_case_name, l_max_transaction_num, 'MODE', systimestamp - l_op_start_tsltz);
   
    commit;    
   
end;
/



--******** CONTROL ****************************************************************

create procedure THINNING_ALL_T is
v1 number;
v_case_name varchar2(32);
begin

    rollback;
    
    v_case_name := to_char (sysdate, 'YYYY-MM-DD HH24:MI:SS');
    
--    delete THINNING_LOG;
--    commit;

    select trunc (log (2, count (*))) into v1 from TRANSACTIONS;
    
    for p in 14..least (24, v1)
    loop
    
    
        THINNING_ALL_SIMP_T (power (2, p), v_case_name);
        THINNING_ALL_CALC_T (power (2, p), v_case_name);
        THINNING_ALL_UDAF_T (power (2, p), v_case_name);
        THINNING_ALL_PPTF_T (power (2, p), v_case_name);
        --if p <= 13 then
        --THINNING_ALL_MODE_T (power (2, p), v_case_name);
        --end if;        
        
    end loop;


end;
/
