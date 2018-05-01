/*
drop package DOWNLOAD_P;
drop type STOCKS_T;
drop type STOCK_T;
drop type TRANSACTION_RAW_LIST_T;
drop type TRANSACTION_RAW_T;
*/




create type TRANSACTION_RAW_T is object (UT number, SEQ_NUM number, APRICE number, AVOLUME number);
create type TRANSACTION_RAW_LIST_T is table of TRANSACTION_RAW_T;

--******************************************************************************

create type STOCK_T is object (STOCK_NAME varchar2(32), ARCHDATE_TSLTZ timestamp with local time zone, ARCHSIZE integer);
create type STOCKS_T as table of STOCK_T;


create or replace package DOWNLOAD_P as

procedure POLL_DIRECTORY_T;
procedure POLL_TRANSACTIONS_T;

end;
/


create or replace package body DOWNLOAD_P as

function HTTP_GET_FILE (p_url varchar2) return clob as
  req   UTL_HTTP.REQ;
  resp  UTL_HTTP.RESP;
  value VARCHAR2(1024);
  result clob;
begin

    begin
        req := utl_http.begin_request (p_url);
        utl_http.set_header (req, 'User-Agent', 'https://yaroslavbat.wordpress.com;https://github.com/yaroslavbat/thinning');
        resp := utl_http.get_response (req);
        loop
            utl_http.read_line (resp, value, TRUE);
            result := result || value || chr(13);
        end loop;
        utl_http.end_response (resp);
    exception when utl_http.end_of_body then
        utl_http.end_response (resp);
    end;

    return result;

end;


procedure POLL_DIRECTORY_T is
    s_proc_start_tsltz timestamp with local time zone default systimestamp;
    strlist sys.odcivarchar2list;
    l_poll_start_tsltz timestamp with local time zone;
    l_poll_end_tsltz timestamp with local time zone;
    l_filebody clob;
    l_first_pos integer;
    l_last_pos integer;
    l_stocks STOCKS_T;
    l_regexp varchar2(1000) default '(<a href=")(.*?)(.csv.gz">)(.*?)(.csv.gz)(.*?)([[:digit:]]{1,2}-[[:alpha:]]{3}-[[:digit:]]{4} [[:digit:]]{2}:[[:digit:]]{2})(.*?)([[:digit:]]{1,12})';
    l_poll_id number;
    l_oraerrnum integer;
    l_oraerrmes varchar2(4000);    
begin

    rollback;
    
    insert into POLLS_DIRECTORY (ID, POLL_START_TSLTZ)
    values (POLLS_DIRECTORY_S.nextval, s_proc_start_tsltz)
    returning ID into l_poll_id;   

    l_filebody := HTTP_GET_FILE('http://api.bitcoincharts.com/v1/csv/');

    l_first_pos := 0;
    strlist := sys.odcivarchar2list ();

    loop
        l_last_pos := instr (l_filebody, chr(13), l_first_pos + 1);
        exit when l_last_pos = 0;
        strlist.extend();
        strlist(strlist.last) := substr (l_filebody, l_first_pos+1, l_last_pos-l_first_pos-1);
        l_first_pos := l_last_pos;
    end loop;

    with 
      T1 as (select 
                    regexp_substr (COLUMN_VALUE, l_regexp, 1, 1, null, 2) as A2
                  , regexp_substr (COLUMN_VALUE, l_regexp, 1, 1, null, 4) as A4
                  , regexp_substr (COLUMN_VALUE, l_regexp, 1, 1, null, 7) as A7
                  , regexp_substr (COLUMN_VALUE, l_regexp, 1, 1, null, 9) as A9
             from table (strlist))
    select STOCK_T (A2, from_tz (to_timestamp (A7, 'DD-Mon-YYYY HH24:MI', 'NLS_DATE_LANGUAGE=AMERICAN'), 'GMT'), A9)
    bulk collect into l_stocks
    from T1 where A2 = A4;
    
    merge into REF_STOCKS a
    using table (l_stocks) b on (a.ANAME = b.STOCK_NAME)
    when not matched then insert (ID, ANAME, IS_ACTIVE, CREATED_TSLTZ) values (REF_STOCKS_S.nextval, b.STOCK_NAME, 'Y', systimestamp);

    insert into POLLS_DIRECTORY_STOCKS (STOCK_ID, POLL_ID, ARCHDATE_TSLTZ, ARCHSIZE)
    select a.ID, l_poll_id, b.ARCHDATE_TSLTZ, b.ARCHSIZE
    from REF_STOCKS a
    join table (l_stocks) b on (a.ANAME = b.STOCK_NAME);
    
    update POLLS_DIRECTORY
    set DURATION_N = THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s_proc_start_tsltz), ARESULT = 'S'
    where ID = l_poll_id;
    
    
    update REF_STOCKS set IS_ACTIVE = 'N' where IS_ACTIVE = 'Y' and CONTIGUOUS_M_COUNT >= 5;

    commit;

exception when others then

    l_oraerrnum := sqlcode;
    l_oraerrmes := sqlerrm;    
    
    update POLLS_DIRECTORY
    set DURATION_N = THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s_proc_start_tsltz), ARESULT = 'X'
      , ORAERRNUM = l_oraerrnum, ORAERRM = l_oraerrmes
    where ID = l_poll_id;
    
    commit;

end;



procedure POLL_TRANSACTIONS_INTERNAL_T (p_StockId number) is
    s_proc_start_tsltz timestamp with local time zone default systimestamp;
    l_poll_start_tsltz timestamp with local time zone;
    l_poll_end_tsltz timestamp with local time zone;
    l_stockname varchar2(128); 
    l_filebody clob;
    l_filesize integer;
    strlist sys.odcivarchar2list;
    l_firstpos integer;
    l_lastpos integer;
    trans TRANSACTION_RAW_LIST_T;

    l_last_max_ut number;
    l_requested_ut number;
    l_received_min_ut number;
    l_received_max_ut number;
    l_received_count integer;
    l_request_url varchar2(1000);

    l_oraerrnum number;
    l_oraerrmsg varchar2(4000);
    
    l_log_id number;
    l_status char (1);
    l_locked_id number;
    
begin
    rollback;
    
    -- Just locking    
    
    begin
        select ID into l_locked_id from REF_STOCKS where ID = p_StockId for update skip locked;
    exception when no_data_found then
        return;    
    end;

    -- Lock succeeded. Preparing...
    
    select ANAME, nvl (MAX_UT, 0) into l_StockName, l_last_max_ut
    from REF_STOCKS  where ID = p_StockId;
    
    
    -- You need to request last second again, because if last received second
    -- contains multiple transaction, some last of this last second transactions
    -- may not feet in generated file due to 20000 rows limit,
    -- but second will be marked as 'received'
      
    l_requested_ut := l_last_max_ut;
    
    l_request_url := 'api.bitcoincharts.com/v1/trades.csv?symbol=' || l_StockName || chr(38) || 'start=' || ltrim (to_char (l_requested_ut));

    l_poll_start_tsltz := systimestamp;
    l_filebody := HTTP_GET_FILE(l_request_url);
    l_poll_end_tsltz := systimestamp;


    if l_filebody is null
    then

        insert into POLLS_TRANSACTIONS (STOCK_ID, POLL_TSLTZ, ARESULT, REQUESTED_UT, POLL_DURATION_N, ALL_DURATION_N, REQUEST_URL, ORAERRNUM, ORAERRMSG)
        values (p_StockId, s_proc_start_tsltz, 'M', l_requested_ut 
              , THINNING.TLS_P.INTERVALDS2NUMBER (l_poll_end_tsltz - l_poll_start_tsltz)
              , THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s_proc_start_tsltz)
              , l_request_url, null, 'FILEBODY IS NULL');

        update REF_STOCKS set LAST_POLL_TSLTZ = s_proc_start_tsltz
                            , LAST_POLL_RESULT = 'M'   -- RESULT = eMpty
                            , CONTIGUOUS_M_COUNT = nvl(CONTIGUOUS_M_COUNT, 0) + 1  
        where ID = p_StockId;    

        commit;
        
        return;

    end if;


    -- Parsing received file

    l_filesize := length (l_filebody);

    l_firstpos := 0;
    strlist := sys.odcivarchar2list ();

    loop
        l_lastpos := instr (l_filebody, chr(13), l_firstpos + 1);
        exit when l_lastpos = 0;
        strlist.extend();
        strlist(strlist.last) := substr (l_filebody, l_firstpos + 1, l_lastpos - l_firstpos - 1);
        l_firstpos := l_lastpos;
    end loop;

    with
      T1 as (select to_number (substr (COLUMN_VALUE, 1, instr (COLUMN_VALUE, ',') - 1)) as UT
                  , rownum - 1 as N
                  , to_number (substr (COLUMN_VALUE, instr (COLUMN_VALUE, ',') + 1, instr (COLUMN_VALUE, ',', 1, 2) - instr (COLUMN_VALUE, ',')-1)) as APRICE 
                  , to_number (substr (COLUMN_VALUE, instr (COLUMN_VALUE, ',', 1, 2)+1)) as AVOLUME
             from table (strlist))
    select TRANSACTION_RAW_T (UT
                            , row_number () over (partition by UT order by N) - 1
                            , APRICE
                            , AVOLUME)
    bulk collect into trans
    from T1;

    select min (UT), max (UT), count (*) into l_received_min_ut, l_received_max_ut, l_received_count from table (trans);

    -- Delete last received second, which can be incomplete
    delete --+ index_asc(a TRANSACTIONS_PKIOT) 
           TRANSACTIONS a
    where STOCK_ID = p_StockId and UT >= l_last_max_ut;
        
    insert into TRANSACTIONS (STOCK_ID, UT, SEQ_NUM, APRICE, AVOLUME, IS_LIVE)
    select p_StockId, UT, SEQ_NUM, APRICE, AVOLUME, 'Y'
    from table (trans);
            
    if l_received_min_ut <> l_received_max_ut
    then
        l_status := 'S';  -- ARESULT = Success
    else
        l_status := 'M';  -- ARESULT = eMpty
    end if;

    insert into POLLS_TRANSACTIONS (STOCK_ID, POLL_TSLTZ, ARESULT, REQUESTED_UT, RECEIVED_MIN_UT, RECEIVED_MAX_UT, FILESIZE, CNT_LINES, POLL_DURATION_N, ALL_DURATION_N, REQUEST_URL)
    values (p_StockId, s_proc_start_tsltz, l_status, l_requested_ut, l_received_min_ut, l_received_max_ut, l_filesize, l_received_count
          , THINNING.TLS_P.INTERVALDS2NUMBER (l_poll_end_tsltz - l_poll_start_tsltz)
          , THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s_proc_start_tsltz)
          , l_request_url);
          
    update REF_STOCKS set LAST_POLL_TSLTZ = s_proc_start_tsltz
                        , LAST_POLL_RESULT = l_status
                        , CONTIGUOUS_M_COUNT = decode (l_status, 'M', nvl(CONTIGUOUS_M_COUNT, 0) + 1)
                        , MAX_UT = decode (l_status, 'S', l_received_max_ut, MAX_UT)
    where ID = p_StockId;    

   
    commit;
    
exception when others then

    l_oraerrmsg := sqlerrm;
    l_oraerrnum := sqlcode;

    rollback;

    insert into POLLS_TRANSACTIONS (STOCK_ID, POLL_TSLTZ, ARESULT, REQUESTED_UT, RECEIVED_MIN_UT, RECEIVED_MAX_UT, FILESIZE, CNT_LINES, POLL_DURATION_N, ALL_DURATION_N, REQUEST_URL
                                  , ORAERRNUM, ORAERRMSG)
    values (p_StockId, s_proc_start_tsltz, 'X', l_requested_ut, l_received_min_ut, l_received_max_ut, l_filesize, l_received_count
          , THINNING.TLS_P.INTERVALDS2NUMBER (l_poll_end_tsltz - l_poll_start_tsltz)
          , THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s_proc_start_tsltz)
          , l_request_url
          , l_oraerrnum, l_oraerrmsg);
        
    commit;

end;


procedure POLL_TRANSACTIONS_T is
    l_StockIds sys.odcinumberlist;
begin


    with
      T1 as (select ID
                  , LAST_POLL_TSLTZ + numtodsinterval (decode (LAST_POLL_RESULT, 'X', 5, 'M', 15, 'S', 0, null, 0), 'minute') as NEXT_POLL_TSLTZ  
             from REF_STOCKS
             where IS_ACTIVE = 'Y')
    select ID bulk collect into l_stockIds
    from T1 
    where NEXT_POLL_TSLTZ < systimestamp or NEXT_POLL_TSLTZ is null
    order by NEXT_POLL_TSLTZ, ID;


    for i in 1..l_StockIds.count
    loop
    
        POLL_TRANSACTIONS_INTERNAL_T (l_StockIds (i));

    end loop;
end;

end;




create or replace package PROCESS_P is

    g_alert_name varchar2(32) default 'DATA_WAS_UPDATED';
    g_debug char(1) default 'N';

    procedure THIN_ALL_T;
    procedure THIN_ALL_MANUAL_T;

end;
/

create or replace package body PROCESS_P is

type THIN_RESULT_T is record  (cnt_Stales          number
                             , cnt_StalesDist      number
                             , cnt_Ins             number
                             , cnt_Upd             number
                             , cnt_Del             number
                             , cnt_Del_Tail        number
                             , dur_Lock            number
                             , dur_C1              number
                             , dur_C2              number
                             , dur_All             number
                                    );

procedure THIN_QUANTUM_INTERNAL (v_Stales STALE_LIST_T, l_tr in out nocopy THIN_RESULT_T) is

cursor c1 is select --+ ordered index_asc (d TRANSACTIONS_PKIOT)
                    0                           as STRIPE_ID
                  , d.STOCK_ID                  as STOCK_ID
                  , d.UT                        as UT_PARENT
                  , d.SEQ_NUM                   as UT
                  , d.APRICE                    as AOPEN
                  , d.APRICE                    as AMIN
                  , d.APRICE                    as AMAX
                  , d.APRICE                    as ACLOSE
                  , d.AVOLUME                   as AVOLUME
                  , 1                           as ACOUNT
                  , d.rowid                     as R                                                 
             from table (v_Stales) c
                  --(select distinct STRIPE_ID, STOCK_ID, UT from STALES) c
             join TRANSACTIONS d on (c.STOCK_ID = d.STOCK_ID and c.UT = d.UT) 
             where c.STRIPE_ID = 1
             union all
             select --+ ordered index_asc(b QUOTES_PKIOT)
                    b.STRIPE_ID, b.STOCK_ID, a.UT, b.UT, b.AOPEN, b.AMIN, b.AMAX, b.ACLOSE, b.AVOLUME, b.ACOUNT   -- TODO
                  , b.rowid as R
             from table (v_Stales) a
                  --(select distinct STRIPE_ID, STOCK_ID, UT from STALES) a
             join QUOTES b on (a.STRIPE_ID - 1 = b.STRIPE_ID and a.STOCK_ID = b.STOCK_ID and a.UT = b.UT_PARENT)
             where a.STRIPE_ID >= 2
             order by 1, 2, 3, 4
             ;


cursor c2 is select --+ ordered index_asc(b QUOTES_PKIOT)
                    b.STRIPE_ID, b.STOCK_ID, b.UT_PARENT, b.UT, b.AOPEN, b.AMIN, b.AMAX, b.ACLOSE, b.AVOLUME, b.ACOUNT, b.rowid as R 
             from table (v_Stales) a
                  --(select distinct STRIPE_ID, STOCK_ID, UT from STALES) a
             join QUOTES b on (a.STRIPE_ID = b.STRIPE_ID and a.STOCK_ID = b.STOCK_ID and THINNING.TLS_P.TRUNC_UT (a.UT, a.STRIPE_ID + 1) = b.UT_PARENT and a.UT = b.UT)
             order by 1, 2, 3, 4
              -- for update of b.ACOUNT -- UNCOMMENT FOR PARALLEL ACCESS
             ;

type QUOTERECORD_T is record (STRIPE_ID             number
                            , STOCK_ID              number
                            , UT_PARENT             number
                            , UT                    number
                            , AOPEN                 number
                            , AMIN                  number
                            , AMAX                  number
                            , ACLOSE                number
                            , AVOLUME               number
                            , ACOUNT                number
                            , R                     urowid);

c2_new      QUOTERECORD_T;
c1rec       QUOTERECORD_T;
c1rec_prev  QUOTERECORD_T;
c2rec       QUOTERECORD_T;
s           timestamp with local time zone;

    procedure FETCH_C2__FETCHED_EARLIER__DEL (p_IsTail char) is
    begin
        delete from QUOTES
        --where current of c2;           
        where rowid = c2rec.R;
                   
        if p_IsTail = 'Y'
        then l_tr.cnt_Del_Tail := l_tr.cnt_Del_Tail + 1;
        else l_tr.cnt_Del := l_tr.cnt_Del + 1;
        end if;
             
    end;

    procedure FETCH_C2__FETCHED_EARLIR is
    begin
        FETCH_C2__FETCHED_EARLIER__DEL ('N');
        fetch c2 into c2rec;
    end;

    procedure FETCH_C2__FETCHED_SAME is
    begin
        if c2_new.ACOUNT <> 0
        then
            if c2rec.AOPEN   <> c2_new.AOPEN  or c2rec.AMIN    <> c2_new.AMIN    or c2rec.AMAX   <> c2_new.AMAX
            or c2rec.ACLOSE  <> c2_new.ACLOSE or c2rec.AVOLUME <> c2_new.AVOLUME or c2rec.ACOUNT <> c2_new.ACOUNT
            then 
                update QUOTES
                set AOPEN  = c2_new.AOPEN,  AMIN    = c2_new.AMIN,    AMAX   = c2_new.AMAX
                  , ACLOSE = c2_new.ACLOSE, AVOLUME = c2_new.AVOLUME, ACOUNT = c2_new.ACOUNT
--                where current of c2;
                where rowid = c2rec.R;
                l_tr.cnt_Upd := l_tr.cnt_Upd + 1;    
            end if;
        else
            FETCH_C2__FETCHED_EARLIER__DEL ('N');     
        end if;
            
        fetch c2 into c2rec;

    end;

    procedure FETCH_C2__FETCHED_LATEST is
    begin
        if c2_new.ACOUNT > 0
        then
            insert into QUOTES (STRIPE_ID, STOCK_ID, UT_PARENT, UT, AOPEN, AMIN, AMAX, ACLOSE, AVOLUME, ACOUNT)
            values (c2_new.STRIPE_ID, c2_new.STOCK_ID, c2_new.UT_PARENT, c2_new.UT
                  , c2_new.AOPEN, c2_new.AMIN, c2_new.AMAX, c2_new.ACLOSE
                  , c2_new.AVOLUME, c2_new.ACOUNT);
            l_tr.cnt_Ins := l_tr.cnt_Ins + 1;
        end if;    
    end;

    procedure FETCH_C2 is
    begin

        loop
    
            case
            when c2%found and (     (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID = c2_new.STOCK_ID and c2rec.UT_PARENT = c2_new.UT_PARENT and c2rec.UT < c2_new.UT)
                                 or (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID = c2_new.STOCK_ID and c2rec.UT_PARENT < c2_new.UT_PARENT)
                                 or (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID < c2_new.STOCK_ID)
                                 or (c2rec.STRIPE_ID < c2_new.STRIPE_ID))
            then
                FETCH_C2__FETCHED_EARLIR;
            
            when c2%found and c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID = c2_new.STOCK_ID and c2rec.UT_PARENT = c2_new.UT_PARENT and c2rec.UT = c2_new.UT -- выбрана такая-же запись
            then
                FETCH_C2__FETCHED_SAME;
                exit;
             
            when c2%found and (     (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID = c2_new.STOCK_ID and c2rec.UT_PARENT = c2_new.UT_PARENT and c2rec.UT > c2_new.UT)
                                 or (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID = c2_new.STOCK_ID and c2rec.UT_PARENT > c2_new.UT_PARENT)
                                 or (c2rec.STRIPE_ID = c2_new.STRIPE_ID and c2rec.STOCK_ID > c2_new.STOCK_ID)
                                 or (c2rec.STRIPE_ID > c2_new.STRIPE_ID ))
            then
                FETCH_C2__FETCHED_LATEST;
                exit;

            when c2%notfound
            then
                FETCH_C2__FETCHED_LATEST;
                exit;

            end case;

            exit when c2%notfound;
                                    
        end loop;
    end;

    procedure INIT_C2 is
    begin
        c2_new.STRIPE_ID    := c1rec.STRIPE_ID + 1;
        c2_new.STOCK_ID     := c1rec.STOCK_ID;
        c2_new.UT_PARENT    := THINNING.TLS_P.TRUNC_UT (c1rec.UT_PARENT, c1rec.STRIPE_ID + 2);
        c2_new.UT           := THINNING.TLS_P.TRUNC_UT (c1rec.UT_PARENT, c1rec.STRIPE_ID + 1);
        c2_new.AOPEN        := c1rec.AOPEN;
        c2_new.AMIN         := null;
        c2_new.AMAX         := null;
        c2_new.ACLOSE       := null;
        c2_new.AVOLUME      := 0;
        c2_new.ACOUNT       := 0;    
    end;

    procedure CLOSE_C2 is
    begin
        c2_new.ACLOSE := c1rec_prev.ACLOSE;
        FETCH_C2;
    end;


    procedure UPDATE_C2 is
    begin
    --    if c1rec.AOPEN is not null
    --    then

            if c1rec.AMIN < c2_new.AMIN or c2_new.AMIN is null
            then c2_new.AMIN := c1rec.AMIN;
            end if; 

            if c1rec.AMAX > c2_new.AMAX or c2_new.AMAX is null
            then c2_new.AMAX := c1rec.AMAX;
            end if;

            c2_new.AVOLUME  := c2_new.AVOLUME + c1rec.AVOLUME;
            c2_new.ACOUNT   := c2_new.ACOUNT + c1rec.ACOUNT;
    --    end if;
    end;


    procedure FETCH_C1 is
    begin
        loop
            c1rec_prev := c1rec;
            fetch c1 into c1rec;
        
            case when c1%notfound and c1rec_prev.STOCK_ID is     null
                 then exit;

                 when c1%notfound and c1rec_prev.STOCK_ID is not null
                 then CLOSE_C2;
                      exit;

                 when c1%found    and c1rec_prev.STOCK_ID is     null
                 then INIT_C2;
                      UPDATE_C2;

                 when c1%found    and c1rec_prev.STOCK_ID is not null
                      and c1rec_prev.STRIPE_ID = c1rec.STRIPE_ID and c1rec_prev.STOCK_ID = c1rec.STOCK_ID and c1rec_prev.UT_PARENT = c1rec.UT_PARENT and c1rec_prev.UT < c1rec.UT 
                 then UPDATE_C2; 

                 when c1%found    and c1rec_prev.STOCK_ID is not null
                 then if ((c1rec_prev.STRIPE_ID = c1rec.STRIPE_ID and c1rec_prev.STOCK_ID = c1rec.STOCK_ID and c1rec_prev.UT_PARENT < c1rec.UT_PARENT)
                       or (c1rec_prev.STRIPE_ID = c1rec.STRIPE_ID and c1rec_prev.STOCK_ID < c1rec.STOCK_ID)
                       or (c1rec_prev.STRIPE_ID < c1rec.STRIPE_ID))
                      then
                           CLOSE_C2;
                           INIT_C2;
                           UPDATE_C2;
                      else
                          raise_application_error (-20001, 'Wrong sort order in cursor');
                      end if;
                 else raise_application_error (-20001, 'Unknown branch in code');
            end case;
        end loop;
    end;

begin

    s := systimestamp;
    open c1;
    l_tr.dur_C1 := THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s);
    s := systimestamp;
    open c2;
    l_tr.dur_C2 := THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - s);


    l_tr.cnt_Ins := 0;
    l_tr.cnt_Upd := 0;
    l_tr.cnt_Del := 0;
    l_tr.cnt_Del_Tail := 0;
    

    fetch c2 into c2rec;
    
    loop
        FETCH_C1;
        exit when c1%notfound;
    end loop;
    
    close c1;

    if c2%found then
        FETCH_C2__FETCHED_EARLIER__DEL ('Y');
    end if; 

    loop
        fetch c2 into c2rec;
        exit when c2%notfound;

        FETCH_C2__FETCHED_EARLIER__DEL ('Y');

    end loop;

    close c2;
    
end;



procedure THIN_QUANTUM (o_result out number) is

    s_ProcStartTSLTZ timestamp with local time zone default systimestamp;
    
    l_StalesNonDistinct STALE_LIST_T;
    l_Stales STALE_LIST_T;

    l_opstart timestamp with local time zone;
    l_opstart_all timestamp with local time zone;
    errm varchar2(4000);

    l_log_id number;
    l_tr THIN_RESULT_T;

begin

    -- Locking MLOG rows ->

    l_opstart_all := systimestamp;

    l_opstart := systimestamp;

    delete --+ index_asc(a STALES_PKIOT)
           STALES a
    where STRIPE_ID <= 18
    returning STALE_T (STRIPE_ID, STOCK_ID, UT) bulk collect into l_StalesNonDistinct;

    l_tr.dur_Lock := THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - l_opstart);
    l_tr.cnt_Stales := l_StalesNonDistinct.count;


    if l_tr.cnt_Stales = 0
    then
        o_result := 0;
        return;
    end if; 
    
    select STALE_T (STRIPE_ID, STOCK_ID, UT) bulk collect into l_Stales from (select distinct STRIPE_ID, STOCK_ID, UT from table (l_StalesNonDistinct));
    l_tr.cnt_StalesDist := l_Stales.count;

    -- Locking MLOG rows <-


    -- Performig thinning ->

    THIN_QUANTUM_INTERNAL (l_Stales, l_tr);

    -- Performig thinning <-

    l_tr.dur_all := THINNING.TLS_P.INTERVALDS2NUMBER (systimestamp - l_opstart_all);    

    insert into THINNING_LOG (ID, START_TSLTZ
                            , CNT_STALES, CNT_STALES_DIST, CNT_INS, CNT_UPD, CNT_DEL, CNT_DEL_TAILS
                            , DUR_LOCK, DUR_OPEN_C1, DUR_OPEN_C2, DUR_ALL)
    values (THINNING_LOG_S.nextval, l_opstart_all
          , l_tr.cnt_Stales, l_tr.cnt_StalesDist, l_tr.cnt_Ins, l_tr.cnt_Upd, l_tr.cnt_Del, l_tr.cnt_Del_Tail
          , l_tr.dur_Lock, l_tr.dur_C1, l_tr.dur_C2, l_tr.dur_All);
              
    o_result := 1;
    
    return;
    
end;

procedure THIN_ALL_T is
    l_message varchar2(128);
    l_status integer;
    result number;
begin
    rollback;
    
    dbms_alert.register (g_alert_name);    
    
    loop
        dbms_alert.waitone (g_alert_name, l_message, l_status, 5);
        if l_status = 0
        then
            loop
                THIN_QUANTUM (result);
                exit when result = 0;
                THINNING.TLS_P.COMMIT_PERIODICAL (60);
            end loop;
        end if;
        
        commit;
        
    end loop;

    commit;
end;



procedure THIN_ALL_MANUAL_T is
    result number;
begin
    rollback;
    
    loop
        THIN_QUANTUM (result);
        exit when result = 0;
        THINNING.TLS_P.COMMIT_PERIODICAL (60);
    end loop;

    commit;
end;

end;
/

exec SYS.DBMS_SCHEDULER.CREATE_JOB (job_name => 'THINNING_LIVE.THINNING_J'          , repeat_interval => 'FREQ=SECONDLY;INTERVAL=10', job_type => 'STORED_PROCEDURE', job_action => 'THINNING_LIVE.PROCESS_P.THIN_ALL_T', enabled => false);
exec SYS.DBMS_SCHEDULER.CREATE_JOB (job_name => 'THINNING_LIVE.POLL_DIRECTORY_J'    , repeat_interval => 'FREQ=HOURLY;INTERVAL=1',   job_type => 'STORED_PROCEDURE', job_action => 'THINNING_LIVE.DOWNLOAD_P.POLL_DIRECTORY_T' , enabled => false);
exec SYS.DBMS_SCHEDULER.CREATE_JOB (job_name => 'THINNING_LIVE.POLL_TRANSACTIONS_J1', repeat_interval => 'FREQ=MINUTELY;INTERVAL=1;BYSECOND=0', job_type => 'STORED_PROCEDURE', job_action => 'THINNING_LIVE.DOWNLOAD_P.POLL_TRANSACTIONS_T', enabled => false);


