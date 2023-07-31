
%inc '/projects/acm_db/prog/alpha_cosmos_mapping_macro.sas';

%global cfh_data;

%let today=%sysfunc(today());
%put today=%sysfunc(putn(&today,ddmmyy10.));

%let weekday=%sysfunc(weekday(&today));
%put weekday=%sysfunc(strip(%sysfunc(putn(&today,downame.))));

%let start_day=%eval(&today-2);
%put start_day=%sysfunc(putn(&start_day,ddmmyy10.));

%let end_day=%eval(&today-1);
%put end_day=%sysfunc(putn(&end_day,ddmmyy10.));

%let report_day=%sysfunc(putn(&end_day,yymmddn8.));
%put report_day=&report_day;

%let path_to_raw_file=/mnt/netapp/SAS/cfh/orders_data/Daily;
%put path_to_raw_files=&path_to_raw_file;

%let output_path=/home/sas/data/regulation/RTS24;
%put output_path=&output_path;

data _null_;
 rc=system("rm &output_path./*.csv");
 rc=system("rm &output_path./*.zip");
run;

%let hour=21;
%put hour=&hour;

%let rep_mifir_close_hour=%sysfunc(dhms(&end_day,&hour,0,0));
%put rep_mifir_close_hour=%sysfunc(putn(&rep_mifir_close_hour,datetime18.));

data _null_;
 GMT_form=put(&rep_mifir_close_hour,GMT_form.);
  if GMT_form='+1' then do;
     moveTOgmt1='3';
     moveTOgmt2='1';
  end;
  else if GMT_form='-1' then do;
     moveTOgmt1='2';
     moveTOgmt2='0';
  end;
 call symput('gmt_conv1',moveTOgmt1);
 call symput('gmt_conv2',moveTOgmt2);
run;

%put gmt_conv1=&gmt_conv1;
%let gmt_hour1=%eval(&hour+&gmt_conv1);
%put gmt_hour1=&gmt_hour1;

%put gmt_conv2=&gmt_conv2;
%let gmt_hour2=%eval(&hour+&gmt_conv2);
%put gmt_hour2=&gmt_hour2;

proc format;
 invalue gmt_diff 'mt4_mar01'=-&gmt_conv1
                  'mt4_acm01'=-&gmt_conv2
                   other=0;
 invalue gmt_time 'mt4_mar01'=&gmt_hour1
                  'mt4_acm01'=&gmt_hour2
                  other=&hour;
run;

%let cosmos_conds=(rename=(login=act_account_id orderType=OrderTypeName));
%let alpha_conds=(where=(tradeDate=&end_day and filledQuantity>0 and side^=' ') rename=(login=act_account_id));


proc sql noprint;
 select distinct quote(strip(partner_companie_name)) into:company_rts22 separated by ","
  from officies.reportable_entities(where=(rts22_reportable=1));
quit;

%put company_rts22=&company_rts22;

proc sql noprint;
 select distinct quote(strip(label)) into:lei_rts22 separated by ','
  from crm_mod.companies_lei(where=(start in (&company_rts22.)));
quit;

%put lei_rts22=&lei_rts22;



/************************** CFH orders START ***************************/

%macro set_cfh;

   %if %eval(&weekday=2) %then %let rep_cfh_date=%eval(&today-3);
       %else %let rep_cfh_date=%eval(&today-1);

   %let rep_cfh_date=%sysfunc(putn(&rep_cfh_date,yymmddn8.));
   %put rep_cfh_date=&rep_cfh_date;

   %let cfh_file=cfh.rts24_journal_&rep_cfh_date;
   %put cfh_file=&cfh_file;

   %if %sysfunc(exist(&cfh_file)) %then %do;
       %let n_obs_cfh=max;
       %let cfh_data=cfh.rts24_journal_&rep_cfh_date(obs=max);
   %end;
   %else %do;
     %let n_obs_cfh=0;
     %let cfh_data=cfh.rts24_journal_20230205(obs=0);
   %end;

   %put cfh_data=&cfh_data;

%mend set_cfh;

%set_cfh;

data clear_vision_accounts;
 set CRMBO.clear_vision_accounts;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei country_c) key=customer_id/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
       length customer_acc_type id_number partner_companie_name0 $40;
       customer_acc_type=put(customer_account_type,acc_type.);
       partner_companie_name0=partner_companie_name;
       id_number=lei;
       drop lei customer_account_type;
     output;
   end;
   when (%sysrc(_Dsenom)) do;
       _error_=0;
   end;
     otherwise;
   end;
run;


proc sort data=&cfh_data out=orders_journal; by Accountid; run;
proc sort data=cfh.gen_accounts_all out=gen_accounts; by Accountid; run;
proc sort data=clear_vision_accounts; by SourceAccountId; run;


data rts24_orders0;
  merge orders_journal(in=_a)
        gen_accounts(keep=AccountId CounterpartTypeId)
        clear_vision_accounts(in=_c keep=SourceAccountId customer_id id_number partner_companie_name id_number
                                    rename=(SourceAccountId=AccountId));
    by Accountid;
  if _a;
  if CounterpartTypeId^=3;
  orderOpenTime=input(OpenTime,best.);
  if side=1 then side1='SELL';
   else side1='BUY';
  drop side;
  act_account_id=strip(AccountId);
  if id_number not in ('' 'NULL') then do;
     other_counter_type='L';
     other_counter_id=id_number;
  end;
  else do;
     other_counter_type='C';
     other_counter_id=act_account_id;
  end;
run;


proc format;
   value $ rts24_cosmos_orderevent
       "NEW"="NEWO"
       "TRIGGERED"="TRIG"
       "CANCELED"="CAMO"
       "REJECTED"="REMO"
       "REPLACED"="REMA"
       "EXPIRED"="EXPI"
       "PARTIALLY_FILLED"="PARF"
       "COMPLETED"="FILL";

   value rts24_mt5_orderevent
        1="NEWO"
        2="COMO"
        3="PARF"
        4="FILL"
        5="REMO"
        6="EXPI"
        7="NEWO"
        8="REMA"
        9="COMO";

   value rts24_mt5_validityperiod
        0="GTCV"
        1="DAVY"
        2="GTTV"
        3="GTDV";

run;

/************************** CFH orders END ********************************/




/************************** Cosmos Orders START ***************************/
 proc format lib=com cntlout=platform_list;
   select $server_list_pl;
 run;

 data _null_;
     set platform_list(rename=(start=server)) end=_eof;
      call symput('server'!!strip(_n_),strip(server));
       if _eof then call symput('n_server',strip(_n_));
 run;

 %put n_server=&n_server server=&server1;

 %macro set_files(fname_e,startDT,endDT,n_serv,conds,plat);

      %let n_month=%sysfunc(intck(month,&startDT,&endDT));

       %do i=0 %to &n_month;
         %do j=1 %to &n_serv;
            %if "&n_serv"^="1" %then %let serv=&&server&j;
               %else %let serv=&plat;
            %let tmp_file=&fname_e.&serv._%sysfunc(intnx(month,&startDT,&i),yymmn6.);
            %if %sysfunc(exist(&tmp_file)) %then %do; &tmp_file&conds  %end;
          %end;
       %end;

 %mend set_files;


data cosmos_orders_lei;
 set %set_files(fname_e=cosm_db.order_events_,startDT=&start_day,endDT=&end_day,n_serv=&n_server, conds=&cosmos_conds);
  set crm_mod.products_all(keep=act_account_id platform customer_id reg_reporting_type) key=plt/unique;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei test_c country_c citizenship) key=customer_id/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
       if test_c^=1;
       length id_number partner_companie_name0 $40;
       partner_companie_name0=partner_companie_name;
       LegalEntityIdentifier=put(partner_companie_name,comp_lei.);
       id_number=lei;
       trans_time=intnx('hour',transactionTime,input(platform,gmt_diff.),'same');
       trans_date=datepart(trans_time);
       format trans_time datetime18. trans_date ddmmyy10.;
       drop orderType;
    end;
    when (%sysrc(_Dsenom)) do;
       _error_=0;
    end;
     otherwise;
   end;
run;

 data cosmos_orders_all;
  length side $15;
   set cosmos_orders_lei(where=(partner_companie_name in (&company_rts22)))
       cosmos_orders_lei(in=_a where=(id_number in (&lei_rts22)));
    length other_counter_type other_counter_id intragroup_company $50;
    if partner_companie_name='Leadcapital_SY' then do;
       partner_companie_name='Tradetech Alpha';
       finq=1;
    end;
    if id_number not in (' ' 'NULL') then do;
       other_counter_type='L';
       other_counter_id=id_number;
       intragroup_company=put(id_number,$comp_lei_inv.);
       intragroup_ind=put(intragroup_company,$intragroup.);
       uti_intragroup_ind=put(intragroup_company,$uti_intragroup.);
       if intragroup_ind='1' then do;
          hedge=1;
          if _a then counterparty_side_indicator=1;
       end;
       if _a then do;
          other_counter_id=put(partner_companie_name0,$comp_lei.);
          uti_intragroup_ind=put(put(other_counter_id,comp_lei_inv.),$uti_intragroup.);
       end;
    end;
    else do;
         other_counter_type='C';
         other_counter_id=act_account_id;
    end;
    if _a then partner_companie_name=put(id_number,$comp_lei_inv.);
 run;

/****************************** Cosmos Orders END **************************************/




/****************************** MT4 Orders START **************************************/


data mt4_orders;
  set mt4db.mt4_trades_real_%sysfunc(putn(&end_day,yymmn6.))(where=(cmd not in (6 7)));
  act_account_id=strip(LOGIN);
  if datepart(open_time)=%sysfunc(inputn(&report_day,yymmdd10.)) then output;
run;

data mt4_orders_lei;
  set mt4_orders;
  set crm_mod.products_all(keep=act_account_id platform customer_id reg_reporting_type) key=plt/unique;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei test_c country_c citizenship) key=customer_id/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
       if test_c^=1;
       partner_companie_name0=partner_companie_name;
    end;
    when (%sysrc(_Dsenom)) do;
       _error_=0;
    end;
     otherwise;
   end;
run;

 data mt4_orders_all;
  length side $15;
   set mt4_orders_lei(where=(partner_companie_name in (&company_rts22)))
       mt4_orders_lei(in=_a where=(lei in (&lei_rts22)));
    length other_counter_type other_counter_id $50;
    if partner_companie_name='Leadcapital_SY' then do;
       partner_companie_name='Tradetech Alpha';
    end;
    if lei not in (' ' 'NULL') then do;
       other_counter_type='L';
       other_counter_id=lei;
       if _a then do;
          other_counter_id=put(partner_companie_name0,$comp_lei.);
       end;
    end;
    else do;
         other_counter_type='C';
         other_counter_id=act_account_id;
    end;
    if _a then partner_companie_name=put(lei,$comp_lei_inv.);
 run;

/****************************** MT4 Orders END **************************************/



/******************************** MT5 Orders START **************************************//*

data mt5_orders0;
    set mt5db.MT5_f1_orders(where=(datepart(TimeSetup) between &start_day and &end_day));
    act_account_id=strip(login);
    platform=strip(platform);
run;

proc sql;
  create table mt5_orders_lei1 as
   select a.*, case when a.Type in (0 2 4 6) then 'BUY'
                    when a.TYpe in (1 3 5 7) then 'SELL'
                    when a.Type=8 and b.Action="0" then 'BUY'
                    when a.Type=8 and b.Action="1" then 'SELL'
               end as Side
     from mt5_orders_lei as a
     left join mt5db.Mt5_01_trades_info as b
      on a.Order=input(b.Order,20.) and b.serverid="0"
;quit;

/******************************** MT5 Orders END **************************************/




/******************************** Alpha Orders START **************************************/


%macro set_alpha_events;

   %let alpha_events_file=Acm1.Order_events_acm01_%sysfunc(putn(&end_day,yymmn6.));
   %put alpha_events_file=&alpha_events_file;

   %if %sysfunc(exist(&alpha_events_file)) %then %do;
        &alpha_events_file&alpha_conds
   %end;
   %else %do;
        Acm1.Order_events_acm01_202303(obs=0)
   %end;

%mend;

data alpha_orders_lei;
 set %set_alpha_events;
  set crm_mod.products_all(keep=act_account_id platform customer_id reg_reporting_type) key=plt/unique;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei test_c country_c citizenship) key=customer_id/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
       if test_c^=1;
       length id_number partner_companie_name0 $40;
       partner_companie_name0=partner_companie_name;
       LegalEntityIdentifier=put(partner_companie_name,comp_lei.);
       id_number=lei;

       OrderTypeName=OrderType;
       symbol=ap_symbol;
       /*
       transactionTime1=input(transactionTime,DATETIME20.);
       orderOpenTime1=input(orderOpenTime,ANYDTDTM.);
       expirationTime1=input(expirationTime,ANYDTDTM.);
       */
       orderid1=strip(orderid);
       orderChainId1=strip(orderChainId);
       price1=put(price,12.8);
       *format transactionTime1 orderOpenTime1 expirationTime1 datetime20.;
       drop OrderType /*transactionTime orderOpenTime expirationTime*/ orderid orderChainId price;

    end;
    when (%sysrc(_Dsenom)) do;
       _error_=0;
    end;
     otherwise;
   end;
run;

data alpha_orders_all;
 length side $15;
  set alpha_orders_lei(where=(partner_companie_name in (&company_rts22)))
      alpha_orders_lei(in=_a where=(id_number in (&lei_rts22)));
   length other_counter_type other_counter_id intragroup_company $50;
   if partner_companie_name='Leadcapital_SY' then do;
      partner_companie_name='Tradetech Alpha';
      finq=1;
   end;
   if id_number not in (' ' 'NULL') then do;
      other_counter_type='L';
      other_counter_id=id_number;
      intragroup_company=put(id_number,$comp_lei_inv.);
      intragroup_ind=put(intragroup_company,$intragroup.);
      uti_intragroup_ind=put(intragroup_company,$uti_intragroup.);
      if intragroup_ind='1' then do;
         hedge=1;
         if _a then counterparty_side_indicator=1;
      end;
      if _a then do;
         other_counter_id=put(partner_companie_name0,$comp_lei.);
         uti_intragroup_ind=put(put(other_counter_id,comp_lei_inv.),$uti_intragroup.);
      end;
   end;
   else do;
        other_counter_type='C';
        other_counter_id=act_account_id;
   end;
   if _a then partner_companie_name=put(id_number,$comp_lei_inv.);
run;

/******************************** Alpha Orders END ****************************************/



data rts24_orders1;
 length platform asset_class product_id1 product_id2 $50 ExecutedbyName $150;
  set rts24_orders0(in=_a rename=(side1=side)) cosmos_orders_all(in=_b) mt4_orders_all(in=_c)
      alpha_orders_all(in=_d rename=(orderid1=orderid orderChainId1=orderChainId price1=price) where=(Symbol^=' '));
   if _a then do;
     company='Finalto Financial Services';
     platform='clear_vision';
     if in_number='NULL' then ind='INT';
       else ind='LEI';
   end;
   else if _b or _c or _d then do;
     company=put(partner_companie_name,rts22_company.);
   end;
   rts24_lei=put(company,$comp_lei.);
   if platform^='clear_vision' then InstrumentSymbol=upcase(Symbol);
     else InstrumentSymbol=put(input(InstrumentId,best.),cfh_SymbolInstrument.);

   if platform='clear_vision' then do;
      product_id1=put(InstrumentSymbol,Cfh_asset_class.);
      product_id2=put(InstrumentSymbol,$Cfh_contract_type.);
   end;
   else do;
      product_id1=put(InstrumentSymbol,$alpha_product_id_a.);
      if product_id1=' ' then product_id1=put(InstrumentSymbol,EMIR8F.);
      product_id2=put(InstrumentSymbol,$alpha_product_id_b.);
      if product_id2=' ' then product_id2=put(InstrumentSymbol,EMIR15F.);
   end;

   if index(upcase(platform),'CLEAR_VISION')>0 then do;
      asset_class=product_id1;
      cfi_code=put(InstrumentSymbol,cfh_cfi_code.);
   end;
   else if index(platform,'cosmos')>0 or index(platform,'mt4')>0 or index(platform,'acm')>0 then do;
      asset_class=strip(product_id1);
      if asset_class in ('.' 'ZZZ' ' ') then asset_class=put(InstrumentSymbol,$EMIR8F.);
      cfi_code=put(InstrumentSymbol,EMIR11F.);
   end;
   if index(platform,'cosmos')>0 and asset_class in ('.' 'ZZZ' ' ') then
         asset_class=put(upcase(compress(scan(symbol,1,'.'),'/')),$EMIR8F.);
   underlying_isin=put(InstrumentSymbol,cfh_SymbolUnderlyingISINs.);
   quote_currency=put(InstrumentSymbol,cfh_SymbolQuoteCurrency.);
   price_notation=put(symbol,$alpha_product_price_notation.);
   notional_ccy1=put(InstrumentSymbol,$alpha_quoting_currency.);
   array char_cols[56] $150 tmp1-tmp56;
     do i=1 to dim(char_cols);
       char_cols[i]=" ";
     end;
   drop i;
   tmp2='0';
   if platform='clear_vision' then do;
      tmp1=put(AccountId,cfhLegalEntityIdentifier.);
      tmp3=AccountId;
   end;
   else if index(platform,'cosmos')>0 or index(platform,'acm')>0  then do;
      tmp1=LegalEntityIdentifier;
      tmp3=act_account_id;
   end;
   else if index(platform,'mt4')>0 then do;
       tmp1=put(partner_companie_name0,comp_lei.);
       tmp3=act_account_id;
   end;
   tmp6=ExecutedByName;
   if tmp6='root' then tmp6='ALGO';
   if other_counter_type='L' then tmp4=other_counter_id;
      else tmp4=customer_id;
   if platform='mt4' then tmp4=customer_id;
   tmp8='DEAL';
   if platform='clear_vision' then tmp10=strip(OrderTime1);
     else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then tmp10=strip(orderOpenTime);
       else if index(platform,'mt4')>0 then tmp10=strip(Open_Time);
   if platform='clear_vision' then tmp11=put(Duration,rts24_duration_to_validityPeriod.);
     else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then tmp11=compress(tif)!!"V";
       else if index(platform,'mt4')>0 then tmp11='GTCV';
   if platform='clear_vision' then tmp13=strip(ExpiryDate1);
      else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then tmp13=strip(expirationTime);
        else if index(platform,'mt4')>0 then tmp13=strip(Expiration);
   tmp17='XXXX';

   array currency_fields[2] $50 Symbol InstrumentSymbol;
   array clean_currency[2] $50 Symbol_cl InstrumentSymbol_cl;
   do i=1 to dim(currency_fields);
       clean_currency[i]=compress(scan(currency_fields[i],1,'.'),'/');
   end;
   if platform='clear_vision' then tmp19=put(InstrumentSymbol_cl,$alpha_product_perm_id.);
     else tmp19=put(Symbol_cl,$alpha_product_perm_id.);
   tmp19=compress(tmp19);
   if tmp19='.' then tmp19=' ';
   if platform='clear_vision' then tmp20=InstrumentSymbol;
     else if index(platform,'cosmos')>0 or index(platform,'mt5')>0 or index(platform,'acm')>0 then tmp20=symbol;
       else if platform='mt4' then tmp20=Symbol;
   tmp21=strip(product_id2);
   if tmp21='ZZZ' then tmp21='CD';
   tmp22=asset_class;
   if tmp22='ZZZ' then tmp22='EQ';
   if platform='clear_vision' then do;
     tmp23=underlying_isin;
     tmp24=cfi_code;
     tmp25=strip(Time1);
     tmp26=OrderId;
     tmp27=put(OrderState,rts24_orderstate_to_orderevent.);
     tmp28=upcase(put(OrderType,rts24_ordertype.));
     tmp35=strip(notional_ccy1);
   end;
   else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then do;
     tmp23=put(upcase(symbol),alpha_product_under_id.);
     tmp24=put(symbol,alpha_product_cfi_code.);
     tmp24=put(upcase(symbol),EMIR11F.);
     if tmp24='ZZZ' then tmp24=" ";
     tmp25=strip(orderOpenTime);
     tmp26=OrderId;
     tmp27=put(eventType,rts24_cosmos_orderevent.);
     tmp28=translate(orderTypeName," ",'_');
     tmp35=strip(price_notation);
   end;
   else if index(platform,'mt4')>0 then do;
     tmp23=put(upcase(symbol),alpha_product_under_id.);
     tmp24=put(symbol,alpha_product_cfi_code.);
     tmp25=strip(Open_Time);
     tmp26=Ticket;
     if index(comment,'cancelled') then tmp27="CAMO";
       else tmp27="NEWO";
     tmp35=strip(price_notation);
   end;

   if index(platform,'cosmos')>0 or index(platform,'mt5')>0 or index(platform,'acm')>0 then do;
      if limitPrice^=. then tmp29='LMTO';
        else if stopPrice^=. then tmp29='STOP';
      tmp30=strip(limitPrice);
      if tmp30='.' then tmp30='';
      tmp32=strip(stopPrice);
      if tmp32='.' then tmp32='';
   end;
   array date_cols[56] 8. t1-t56;
   do j=1 to dim(date_cols);
     if j in (10, 13, 25) then do;
       date_cols[j]=input(char_cols[j],20.);
     end;
   end;
   drop j;
   if platform='clear_vision' then tmp34=strip(Price);
     else if index(platform,'mt4')>0 then tmp34=strip(open_price);
       else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then do;
          tmp34=strip(transactionPrice);
          if side='BUY' then tmp38=cats('BUY','I');
            else tmp38=side;
       end;
   if tmp34='.' then tmp34='';
   tmp37='MONE';
   if platform='mt4' then do;
      if cmd in (0 2 4) then tmp38='BUYI';
        else if cmd in (1 3 5) then tmp38='SELL';
   end;
   else do;
      if side='BUY' then tmp38='BUYI';
        else tmp38=side;
   end;
   if index(platform,'cosmos')>0 or index(platform,'acm')>0 then tmp39=strip(isActive);
   tmp40='UNIT';
   if platform='clear_vision' then do;
       tmp42=strip(Amount);
       tmp43=strip(Amount-Filled);
       tmp44=strip(Amount);
       tmp45=strip(Filled);
   end;
   else if index(platform,'cosmos')>0 or index(platform,'acm')>0 then do;
      tmp42=strip(requestedQuantity);
      tmp43=strip(remainingQuantity);
      tmp44=strip(requestedQuantity);
      tmp45=strip(filledQuantity);
   end;
   else if index(platform,'mt4')>0 then do;
      tmp42=strip(Volume);
   end;
   if tmp42='.' then tmp42=" ";
   if tmp43='.' then tmp43=" ";
   if tmp44='.' then tmp44=" ";
   if tmp45='.' then tmp45=" ";
   format t10 t13 t25 datetime20.;
   tmp10=put(t10,datetime20.);
   tmp13=put(t13,datetime20.);
   tmp25=put(t25,datetime20.);
   f_name="&report_day._Finalto_"!!compress(put(company,rts22_country_prefix_code.))!!'_'!!rts24_lei!!'_'!!compress(put(company,rts22_display_name.))!!'_Surveillance_Orders_RTS24';
   file_name="&output_path./"!!f_name;
   drop t1-t56 i j;
run;


data rts24_orders1;
  set rts24_orders1;
   tmp10=compress(tmp10);
   if index(tmp10,'.') then tmp10=' ';
   tmp13=compress(tmp13);
   if index(tmp13,'.') then tmp13=' ';
   tmp25=compress(tmp25);
   if index(tmp25,'.') then tmp25=' ';
   if platform='clear_vision' and tmp4=' ' and tmp1^=' ' then do;
      tmp4=tmp1;
      tmp1='549300FSY1BKNGVUOR59';
   end;
   if order_status='COMPLETED' then tmp27='FILL';
run;


proc sort data=rts24_orders1 nodupkey; by _All_; run;

proc sort data=rts24_orders1(keep=file_name) out=rts24_orders_files nodupkey; by file_name; run;

data rts24_orders2;
  set rts24_orders_files(in=_head keep=file_name) rts24_orders1(keep=file_name tmp1-tmp56);
   if _head then do;
     count=0;
     /*1*/
     tmp1="submittingentity";
     tmp2="dea";
     tmp3='AccountId';
     tmp4="clientid";
     tmp5="decisionid";
     tmp6="executionid";
     tmp7="nonexecutingbroker";
     tmp8="tradingcapacity";
     tmp9="liquidityprovision";
     tmp10="datetime";
     /*11*/
     tmp11="validityperiod";
     tmp12="orderrestriction";
     tmp13="validityperiodtime";
     tmp14="prioritytimestamp";
     tmp15="prioritysize";
     tmp16="sequencenumber";
     tmp17="segmentmic";
     tmp18="orderbookcode";
     tmp19='Permid';
     tmp20="Instrumentname";
     /*21*/
     tmp21="Contract type";
     tmp22="Asset";
     tmp23="Underlying ISIN";
     tmp24="Classification";
     tmp25="dateofreceipt";
     tmp26="orderid";
     tmp27="orderevent";
     tmp28="ordertype";
     tmp29="ordertypeclassification";
     tmp30="limitprice";
     /*31*/
     tmp31="additionallimitprice";
     tmp32="stopprice";
     tmp33="peggedlimitprice";
     tmp34="transactionprice";
     tmp35="pricecurrency";
     tmp36="currency2ndleg";
     tmp37="pricenotation";
     tmp38="buysell";
     tmp39="orderstatus";
     tmp40="quantitynotation";
     /*41*/
     tmp41="quantitycurrency";
     tmp42="initialquantity";
     tmp43="remainingquantity";
     tmp44="displayedquantity";
     tmp45="tradedquantity";
     tmp46="maq";
     tmp47="mes";
     tmp48="mesfirstexecutiononly";
     tmp49="passiveonlyindicator";
     tmp50="passiveaggressiveid";
     /*51*/
     tmp51="selfexecutionprevention";
     tmp52="strategylinkedid";
     tmp53="routingstrategy";
     tmp54="tradingvenuetransactionid";
     tmp55="tradingphases";
     tmp56="indicativeauctionprice";
     tmp57="indicativeauctionvolume";
   end;
   else do;
     count=1;
   end;
   drop t1-t56;
run;

proc sort data=rts24_orders2; by file_name count; run;


data rts24_orders3;
set rts24_orders2;
 by file_name count;
   array char_(*) _CHARACTER_;
   do i=1 to dim(char_);
     char_(i)=translate(char_(i),"'",'"');
   end;
   fv=strip(file_name)!!".csv";
   FILE writeout FILEVAR=fv encoding='utf-8' lrecl=10000 dsd dropover ;
   put tmp1-tmp71;
run;

data _null_;
set rts24_orders_files(in=_a keep=file_name);
  zipcommand="zip -j "!!strip(file_name)!!".zip "!!strip(file_name)!!".csv";
  rc=system(zipcommand);
  put zipcommand=;
  put rc=;
  rc=system("cp "!!strip(file_name)!!".zip /mnt/netapp/SAS/regulation/RTS24/");
  put rc=;
  rc=system("find /mnt/netapp/SAS/regulation/RTS24/ -mindepth 1 -mtime +14 -delete");
  put rc=;
run;


/* Raw Data for FFS *//*

proc sql noprint;
 select name
 into: ffs_raw_varlist SEPARATED by " "
 from dictionary.columns
 where libname='WORK' and memname='RTS24_ORDERS0';

 select file_name, scan(file_name,-1,'/') as f_name
 into: file_name, :f_name  SEPARATED by " "
 from rts24_orders_files(where=(index(file_name,'_FFS_')));
quit;

%put file_name=&file_name;
%put f_name=&f_name;
%put ffs_raw_varlist=&ffs_raw_varlist;

proc export data=rts24_orders0(keep=&ffs_raw_varlist.)
    outfile="&f_name._RAW_DATA.csv" label
    dbms=csv
    replace;
run;

data _null_;
  rc=system("zip &file_name._RAW_DATA.zip &file_name._RAW_DATA.csv");
  put rc=;
  rc=system("cp &file_name._RAW_DATA.zip /mnt/netapp/SAS/regulation/RTS24/");
  put rc=;
run;
*/