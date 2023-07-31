
%inc '/projects/acm_db/prog/alpha_cosmos_mapping_macro.sas';
%inc '/projects/cfh/prog/rates_cfh_macro.sas';
%inc "/projects/trade/prog/mifir_instruments.sas";

%inc "/projects/cfh/cfhregsql/reg_cfh_common_part.sas";

%global rep_cfh_date cfh_data n_obs_cfh;

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

%let hour=21;
%put hour=&hour;

%let rep_close_hour=%sysfunc(dhms(&end_day,&hour,0,0));
%put rep_close_hour=%sysfunc(putn(&rep_close_hour,datetime18.));

data _null_;
 GMT_form=put(&rep_close_hour,GMT_form.);
  if GMT_form='+1' then moveTOgmt='3';
     else if GMT_form='-1' then moveTOgmt='2';
 call symput('gmt_conv',moveTOgmt);
run;

%put gmt_conv=&gmt_conv;

%let gmt_hour=%eval(&hour+&gmt_conv);
%put gmt_hour=&gmt_hour;

proc format;
 invalue gmt_diff 'mt4_mar01'=-&gmt_conv
                   other=0;
 invalue gmt_time 'mt4_mar01'=&gmt_hour
                  other=&hour;
run;

%let html_path=/home/sas/data/regulation/RTS22;
%put html_path=&html_path;

data _null_;
 rc=system("rm &html_path/*.csv");
 rc=system("rm &html_path/*.zip");
run;

%let cond_close=(drop=usd_rate where=(cmd in (0 1)));
%put cond_close=&cond_close;

%let cond_close_mt5=(drop=usd_rate dealer action where=(cmd in (0 1)));
%put cond_close_mt5=&cond_close_mt5;

%let cond_acm=(in=_acm keep=report_date open_dt open_date cur1 cur1_sb cur2 cur2_sb account external_code side open_rate
                            volume volume_lot volume_lot_sb symbol symbol_sb platform_acm customer_id test_c pair_id
                            executor_type executor platform
               rename=(open_dt=open_time1 account=act_account_id external_code=act_pos_id)
               where=(customer_id^=' ' and test_c^=1 and report_date in (&start_day &end_day)));
%put cond_acm=&cond_acm;


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


proc sql noprint;
 select distinct partner_companie_name into:company_mas separated by '","'
  from officies.reportable_entities(where=(mas_reportable=1));
quit;
%put company_mas=&company_mas;

proc sql noprint;
 select distinct label into:lei_mas separated by '","'
  from crm_mod.companies_lei(where=(start in ("&company_mas")));
quit;
%put lei_mas=&lei_mas;

data uti_format;
  start="**OTHER**";
  label=' ';
  fmtname='$cfh_uti';
  HLO='O';
  keep start label fmtname HLO;
run;

proc format cntlin=uti_format; run;

data uti_format;
 set cfh.UTI end=eof;
  where same and TradeDate=&TradeDate;
   start=TradeId;
   label=UTI;
   fmtname='$cfh_uti';
  output;
  if eof then do;
   call symput('n_uti',strip(_n_));
   start='**OTHER**';
   label=' ';
   HLO='O';
   output;
  end;
  keep start label fmtname HLO;
run;

proc sort data=uti_format nodupkey; by start; run;

proc format cntlin=uti_format lib=work; run;


%macro set_cfh;

 %if %eval(&weekday=2) %then %let rep_cfh_date=%eval(&today-3);
     %else %let rep_cfh_date=%eval(&today-1);
 %let rep_cfh_cur_month=%sysfunc(putn(&rep_cfh_date,yymmn6.));
 %let rep_cfh_last_month=%sysfunc(putn(%sysfunc(intnx(month,&rep_cfh_date,-1)),yymmn6.));

 %let cfh_file=cfh.rbook_trade_&rep_cfh_cur_month;
   %if %sysfunc(exist(&cfh_file)) %then %do;
       %let n_obs_cfh=max;
       %let cfh_data=cfh.rbook_trade_&rep_cfh_cur_month(obs=max
                      keep=tradeid SourceAccountId TargetAccountId SrcAcntCcy side amount InstrumentSymbol price eodprice ConversionPrice executiontime TradeDate tradetypeid
                      rename=(amount=volume_lot price=trans_rate executiontime=trans_time))
            ;
       %if %sysfunc(exist(Cfh.Earlier_trade_&rep_cfh_cur_month,data)) %then
        %do;
           %let cfh_data=&cfh_data
                    Cfh.Earlier_trade_&rep_cfh_cur_month(obs=max
                     keep=tradeid SourceAccountId TargetAccountId SrcAcntCcy side amount InstrumentSymbol price eodprice ConversionPrice executiontime TradeDate tradetypeid
                     rename=(amount=volume_lot price=trans_rate executiontime=trans_time))
            ;
        %end;
   %end;
   %else %do;
     %let n_obs_cfh=0;
     %let cfh_data=cfh.rbook_trade_&rep_cfh_cur_month(obs=0
                    keep=tradeid SourceAccountId SrcAcntCcy side amount InstrumentSymbol price eodprice ConversionPrice executiontime TradeDate tradetypeid
                    rename=(amount=volume_lot price=trans_rate executiontime=trans_time));
   %end;

%mend set_cfh;

%set_cfh;


data clear_vision_accounts;
 set CRMBO.clear_vision_accounts;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei) key=customer_id/unique;
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

data clear_vision_accounts_all;
 set clear_vision_accounts(where=(partner_companie_name in (&company_rts22.)))
     clear_vision_accounts(in=_a where=(id_number in (&lei_rts22.)));
  if _a then partner_companie_name=put(id_number,$comp_lei_inv.);
   removeable_brands=put(partner_companie_name,$removeable_brands.);
    if removeable_brands^='' then do;
       brand_del_count=countw(removeable_brands,',');
       array brand_del $50 brand_del1-brand_del10;
         do i=1 to brand_del_count;
            brand_del[i]=strip(scan(removeable_brands,i,','));
           if brand_del[i]=brand then delete;
         end;
     drop i removeable_brands brand_del_count brand_del1-brand_del10;
    end;
run;

data cfh_instruments_formats;
 set cfh.Abook_Instrument;
  fmtname='$CFH_PAIR';
  start=symbol;
  label=pair_id;
run;

proc format cntlin=cfh_instruments_formats; run;


data clear_vision_trades_crmbo(where=(trans_date=&rep_cfh_date)) clear_vision_positions_crmbo(where=(trans_date=&rep_cfh_date));
 SET clear_vision_accounts_all(rename=(zone=platform));
  DO UNTIL (_IORC_ NE 0);
    SET &cfh_data KEY=SourceAccountId;
    _ERROR_ = 0;
    length position_id1 $20 positionkey $50 act_pos_id $30 act_account_id $70 time $30 order_type $30 position_side $4 effect $20
           quantity $10 filled_price $20 usd_rate $10 volume_filled_usd $20 notional_amount1 $20 notional_cur1 $30 notional_amount2 $20
           notional_cur2 $30 pair_id $15 cur1 $30 cur2 $20 symbol $50 DoDelegatedReporting FinancialNature CorporateSector $10;
    position_id1=tradeid;
    positionkey=tradeid;
    act_pos_id=tradeid;
    act_account_id=SourceAccountId;
    DoDelegatedReporting=put(SourceAccountId,$cfhDoDelegatedReporting.);
    FinancialNature=put(SourceAccountId,$cfhFinancialNature.);
    CorporateSector=put(SourceAccountId,$cfhCorporateSector.);
    if DoDelegatedReporting='True' then reg_reporting_type='Delegated';
    action='C';
    time=strip(put(datepart(trans_time),yymmdd10.)||' '||put(timepart(trans_time),time.));
    order_type='INSTANST MARKET';
    effect='CLOSE';
    pair_id=put(InstrumentSymbol,$CFH_PAIR.);
    cur1=put(trim(left(pair_id)),$CUR1F.);
    cur2=put(trim(left(pair_id)),$CUR2F.);
    symbol=InstrumentSymbol;
    if tradetypeid='5' then do;
       if side='B' then position_side='SELL';
          else position_side='BUY';
    end;
    else do;
       if side='B' then position_side='BUY';
          else position_side='SELL';
    end;
    if upcase(position_side)='BUY' then do;
       buy_sell='0';
       amount_s=volume_lot;
    end;
    else do;
      buy_sell='1';
      amount_s=-volume_lot;
    end;
    pl_in_cur2=(eodprice-trans_rate)*amount_s;
    %cur_to_targetcur_f(targetcur='USD',curvar=cur2,datevar=TradeDate,ratevar=USD_rate1);
    trans_date=tradedate;
    quantity=put(volume_lot,8.2);
    usd_rate=put(usd_rate1,12.8);
    volume_filled_usd=put(abs(volume_lot)*trans_rate*usd_rate1,12.8);
    filled_price=put(trans_rate,12.8);
    notional_amount1=quantity;
    notional_cur1=upcase(cur1);
    notional_amount2=strip(volume_lot*trans_rate);
    notional_cur2=strip(upcase(cur2));
    if notional_cur2='CNH' then notional_cur2='CNY';
    format trans_date ddmmyy10.;
   IF _IORC_ = 0 THEN OUTPUT;
  END;
run;

data clear_vision_trades_all(where=(tradedate=&rep_cfh_date)) clear_vision_positions_all(where=(tradedate=&rep_cfh_date))
     clear_vision_trades_mas(where=(tradedate=&rep_cfh_date)) clear_vision_positions_mas(where=(tradedate=&rep_cfh_date));
     length partner_companie_name $50;
 set &cfh_data;
  partner_companie_name='cfh clearing';
  platform='Clear_Vision';
  LegalEntityIdentifier_source=put(SourceAccountId,$cfhLegalEntityIdentifier.);
  LegalEntityIdentifier_target=put(TargetAccountId,$cfhLegalEntityIdentifier.);
  output clear_vision_trades_all;
  output clear_vision_positions_all;
  if LegalEntityIdentifier_source="&lei_mas" or LegalEntityIdentifier_target="&lei_mas" then do;
     partner_companie_name="&company_mas";
     platform='Clear_Vision';
     output clear_vision_trades_mas;
     output clear_vision_positions_mas;
  end;
run;

data clear_vision_all;
 set clear_vision_trades: clear_vision_positions:;
    length position_id1 $20 positionkey $50 act_pos_id $30 act_account_id $70 time $30 order_type $30 position_side $4 effect $20
           quantity $10 filled_price $20 usd_rate $10 volume_filled_usd $20 notional_amount1 $20 notional_cur1 $30 notional_amount2 $20
           notional_cur2 $30 pair_id $15 cur1 $30 cur2 $20 symbol $50 DoDelegatedReporting FinancialNature CorporateSector $10;
    where TradeTypeId not in ('0' '2' '4' '5' '8' '11' '12' '13' '14' '16' '18' '19' '20');
    position_id1=tradeid;
    positionkey=tradeid;
    act_pos_id=tradeid;
    act_account_id=SourceAccountId;
    DoDelegatedReporting=put(SourceAccountId,$cfhDoDelegatedReporting.);
    FinancialNature=put(SourceAccountId,$cfhFinancialNature.);
    CorporateSector=put(SourceAccountId,$cfhCorporateSector.);
    if DoDelegatedReporting='True' then reg_reporting_type='Delegated';
    action='C';
    time=strip(put(datepart(trans_time),yymmdd10.)||' '||put(timepart(trans_time),time.));
    order_type='INSTANST MARKET';
    effect='CLOSE';
    pair_id=put(InstrumentSymbol,$CFH_PAIR.);
    cur1=put(trim(left(pair_id)),$CUR1F.);
    cur2=put(trim(left(pair_id)),$CUR2F.);
    symbol=InstrumentSymbol;
    if tradetypeid='5' then do;
       if side='B' then position_side='SELL';
          else position_side='BUY';
    end;
    else do;
       if side='B' then position_side='BUY';
          else position_side='SELL';
    end;
    if upcase(position_side)='BUY' then do;
       buy_sell='0';
       amount_s=volume_lot;
    end;
    else do;
      buy_sell='1';
      amount_s=-volume_lot;
    end;
    pl_in_cur2=(eodprice-trans_rate)*amount_s;
    %cur_to_targetcur_f(targetcur='USD',curvar=cur2,datevar=TradeDate,ratevar=USD_rate1);
    trans_date=tradedate;
    quantity=put(volume_lot,8.2);
    usd_rate=put(usd_rate1,12.8);
    volume_filled_usd=put(abs(volume_lot)*trans_rate*usd_rate1,12.8);
    filled_price=put(trans_rate,12.8);
    notional_amount1=quantity;
    notional_cur1=upcase(cur1);
    notional_amount2=strip(volume_lot*trans_rate);
    notional_cur2=strip(upcase(cur2));
    if notional_cur2='CNH' then notional_cur2='CNY';
    format trans_date ddmmyy10.;
    CounterpartId_Source=put(SourceAccountId,$cfhCounterpartId.);
    CounterpartId_Target=put(TargetAccountId,$cfhCounterpartId.);
    if LegalEntityIdentifier_source in (&lei_rts22) /*or LegalEntityIdentifier_target in (&lei_rts22)*/;
    if CounterpartId_Target not in ('12600' '13148' '4886');
    if LegalEntityIdentifier_Source='549300FSY1BKNGVUOR59' and LegalEntityIdentifier_Target='549300FSY1BKNGVUOR59' then delete;
run;


%macro set_files(fname_e,start_day,end_day,conds);

   %let n_month=%sysfunc(intck(month,&start_day,&end_day));

    %do i=0 %to &n_month;
      %let tmp_file=&fname_e._%sysfunc(intnx(month,&start_day,&i),yymmn6.);
      %if %sysfunc(exist(&tmp_file)) %then %do; &tmp_file&conds %end;
     %end;

%mend set_files;

%inc "/projects/trade/prog/rates_macro.sas";

data rts22_trades0;
 length position_id1 $20 positionkey $50 act_account_id $25 time $30 order_type $30 position_side $4 effect $20
        symbol $50 quantity $10 requested_price $20 filled_price $20 slippage1 $10 cur2 $20 usd_rate $10 slippage_usd $20
        slippage_volume $15 slippage_volume_usd $20 volume_request_usd $20 volume_filled_usd $20 stp_time $30 stp_price $20
        time_process_start time_process_end $30 speed_of_exec $20 platform $20 notional_amount1 $20 notional_cur1 $30
        notional_amount2 $20 notional_cur2 $30 rate_cur2_cur2_sb1 $50 pl_all $20;
 set %set_files(fname_e=cosm_db.cosmos_trades_mod,start_day=&start_day,end_day=&today,conds=&cond_close)
     Cosm_db.Cosmos_trades_mod_open(in=_o drop=usd_rate where=(cmd in (0 1)))
     %set_files(fname_e=metat.mt4_trades_mod,start_day=&start_day,end_day=&today,conds=&cond_close)
     metat.mt4_trades_mod_open(in=_o drop=usd_rate where=(cmd in (0 1)))
     %set_files(fname_e=mt5db.mt5_trades_mod,start_day=&start_day,end_day=&today,conds=&cond_close_mt5)
     mt5db.mt5_trades_mod_open(in=_o drop=usd_rate where=(cmd in (0 1)))
     %set_files(fname_e=acm1.acm_trade_mod,start_day=&start_day,end_day=&today,conds=&cond_acm);

   if dhms(&start_day,&hour,0,0)<=open_time1<dhms(&end_day,&hour,0,0) then
    do;
       if ordertype='MARKET' then order_type='INSTANT_MARKET';
          else if ordertype='LIMIT' then order_type='LIMIT (TP)';
          else if ordertype='STOP' then order_type='STOP (SL)';
       else order_type=ordertype;
       action='O';
       if _o then do;
          position_id1=act_pos_id;
          order_type='INSTANT_MARKET';
       end;
       else do;
            if index(platform,'cosmos') then position_id1=put(openorderid,best12.);
               else do;
                       position_id1=act_pos_id;
                       positionkey=act_pos_id;
               end;
       end;
       trans_time=open_time1;
       trans_date=open_date;
       trans_rate=open_rate;
       if cmd=0 then
          do;
            position_side='BUY';
            slippage=reqopenprice-open_rate;
          end;
       else if cmd=1 then
          do;
            position_side='SELL';
            volume=-volume;
            slippage=open_rate-reqopenprice;
         end;

       %cur_to_targetcur_f(targetcur=cur2,curvar='USD',enddate=open_date,outratevar=usd_rate1);
       %cur_to_targetcur_f(targetcur=cur2,curvar=cur2_sb,enddate=open_date,outratevar=rate_cur2_cur2_sb);

       effect='OPENING';
       if _o then effect='OPENEING BEFORE';
       slippage1=put(slippage,12.8);
       quantity=put(volume,8.2);
       usd_rate=put(usd_rate1,12.8);
       rate_cur2_cur2_sb1=put(rate_cur2_cur2_sb,12.8);
       slippage_usd=put(slippage*usd_rate1,12.8);
       slippage_volume=put(abs(volume)*slippage,12.8);
       slippage_volume_usd=put(abs(volume)*slippage*usd_rate1,12.8);
       volume_request_usd=put(abs(volume)*reqopenprice*usd_rate1,12.8);
       volume_filled_usd=put(abs(volume)*open_rate*usd_rate1,12.8);
       requested_price=put(reqopenprice,12.8);
       filled_price=put(open_rate,12.8);
       time=strip(put(datepart(open_time1),yymmdd10.)||' '||put(timepart(open_time1),time.));
       stp_time=strip(put(datepart(stpOpenTime),yymmdd10.)||' '||put(timepart(stpOpenTime),time.));
       stp_price=put(stpOpenPrice,12.8);
       time_process_start=strip(put(datepart(Processing_open_start_time),yymmdd10.)||' '||put(timepart(Processing_open_start_time),time.));
       time_process_end=strip(put(datepart(Processing_open_end_time),yymmdd10.)||' '||put(timepart(Processing_open_end_time),time.));
       speed_of_exec=left(round(put(left((Processing_open_end_time-Processing_open_start_time)*1000),8.)));
       notional_amount1=strip(Volume_Lot);
       notional_cur1=upcase(cur1);
       notional_amount2=strip(Volume_Lot*open_rate);
       notional_cur2=strip(upcase(cur2));
       if notional_cur2='CNH' then notional_cur2='CNY';
       pl_all=strip(close_net_profit_loss);
       if ordertype=' ' then order_type='INSTANT_MARKET';
       if _acm then do;
        position_side=side;
          if side='BUY' then do;
             buy_sell='0';
             cmd=0;
          end;
          else do;
               buy_sell='1';
               cmd=1;
          end;


          if symbol='UKO/USD' then symbol='UKOUSD';
          if symbol_sb='UKO/USD' then symbol_sb='UKOUSD';

          %alpha_cosmos_mapping(symbol_var=symbol,pair_id_var=pair_id);

          if pair_id ne 'NO MAP' then do;
             cur1=put(pair_id,$CUR1F.);
             cur2=put(pair_id,$CUR2F.);
          end;

          %alpha_cosmos_mapping(symbol_var=symbol_SB,pair_id_var=pair_id_SB);

          if pair_id_sb ne 'NO MAP' then do;
             cur1_sb=put(pair_id_sb,$CUR1F.);
             cur2_sb=put(pair_id_sb,$CUR2F.);
          end;

          trans_date=datepart(trans_time);
       end;
      output;
    end;
  if dhms(&start_day,&hour,0,0)<=close_time1<dhms(&end_day,&hour,0,0)  then
     do;
       if ordertype='MARKET' then order_type='INSTANT_MARKET';
          else if ordertype='LIMIT' then order_type='LIMIT (TP)';
          else if ordertype='STOP' then order_type='STOP (SL)';
       else order_type=ordertype;

       position_id1=act_pos_id;
       if index(platform,'cosmos')=0 then positionkey=act_pos_id;

       if cmd=0 then
          do;
             position_side='SELL';
             volume=-volume;
             slippage=close_rate-reqcloseprice;
       end;
       else if cmd=1 then do;
            position_side='BUY';
            slippage=reqcloseprice-close_rate;
       end;

       action='C';
       effect='CLOSING';
       slippage1=put(slippage,12.8);
       quantity=put(volume,8.2);
       usd_rate=put(usd_rate1,12.8);
       rate_cur2_cur2_sb1=put(rate_cur2_cur2_sb,12.8);
       slippage_usd=put(slippage*usd_rate1,12.8);
       slippage_volume=put(abs(volume)*slippage,12.8);
       slippage_volume_usd=put(abs(volume)*slippage*usd_rate1,12.8);
       volume_request_usd=put(abs(volume)*reqcloseprice*usd_rate1,12.8);
       volume_filled_usd=put(abs(volume)*close_rate*usd_rate1,12.8);
       requested_price=put(reqcloseprice,12.8);
       filled_price=put(close_rate,12.8);
       time=strip(put(datepart(close_time1),yymmdd10.)||' '||put(timepart(close_time1),time.));
       stp_time=strip(put(datepart(stpCloseTime),yymmdd10.)||' '||put(timepart(stpCloseTime),time.));
       stp_price=put(stpClosePrice,12.8);
       time_process_start=strip(put(datepart(Processing_close_start_time),yymmdd10.)||' '||put(timepart(Processing_close_start_time),time.));
       time_process_end=strip(put(datepart(Processing_close_end_time),yymmdd10.)||' '||put(timepart(Processing_close_end_time),time.));
       speed_of_exec=left(round(put(left((Processing_close_end_time-Processing_close_start_time)*1000),8.)));
       notional_amount1=strip(Volume_Lot);
       notional_cur1=strip(upcase(cur1));
       notional_amount2=strip(Volume_Lot*close_rate);
       notional_cur2=strip(upcase(cur2));
       if notional_cur2='CNH' then notional_cur2='CNY';
       pl_all=strip(close_net_profit_loss);
       if ordertype=' ' then order_type='INSTANT_MARKET';
       trans_time=close_time1;
       trans_date=close_date;
       trans_rate=close_rate;
      output;
     end;
run;


data rts22_trades_lei;
 set rts22_trades0;
  set bidata.acc_all(keep=customer_id partner_companie_name customer_account_type brand lei test_c country_c) key=customer_id/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
       if test_c^=1;
       length customer_acc_type $40;
       customer_acc_type=put(customer_account_type,acc_type.);
       id_number=lei;
       trans_time=intnx('hour',trans_time,input(platform,gmt_diff.),'same');
       trans_date=datepart(trans_time);
       format trans_time datetime18. trans_date ddmmyy10.;
       drop lei customer_account_type test_c;
     output;
   end;
   when (%sysrc(_Dsenom)) do;
       _error_=0;
   end;
     otherwise;
   end;
run;



data rts22_trades_all;
 set clear_vision_all
     rts22_trades_lei(where=(partner_companie_name in (&company_rts22.)) drop=Commission)
     rts22_trades_lei(in=_a where=(id_number in (&lei_rts22.)) drop=Commission);

   length other_counter_type other_counter_id $50;
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
         other_counter_id=put(partner_companie_name,$comp_lei.);
         uti_intragroup_ind=put(put(other_counter_id,comp_lei_inv.),$uti_intragroup.);
      end;
   end;
   else do;
        other_counter_type='C';
        other_counter_id=act_account_id;
   end;
   if _a then partner_companie_name=put(id_number,$comp_lei_inv.);
run;

data rts22_trades_all1;
 set rts22_trades_all;
  set acm1.collateral_all(keep=act_account_id platform intragroup offsetaccount mifidonly account_currency) key=plt/unique;
   select(_iorc_);
     when (%sysrc(_sok)) do;
     output;
   end;
   when (%sysrc(_Dsenom)) do;
       _error_=0;
     call missing(intragroup,offsetaccount,mifidonly,account_currency);
    output;
   end;
     otherwise;
   end;
run;


data rts22_trades1;
 set rts22_trades_all1;
  length tmp1-tmp71 $150 Instrument symbol val_time val_date rts22_name rts22_lei comp_name comp_lei file_name $200
         report_day $15  citizenship $15 passport id_number1 $10 first_name last_name birth_date $20 branch_country $50
         display_ind $1 display_name $50 display_lei $200 display_country_code $10  display_companie_name $50;
  val_time=strip(strip(put(datepart(round(trans_time,1)),yymmddd10.))||'T'||strip(put(timepart(round(trans_time,1)),E8601TM.))||'Z');
  val_date=put(trans_date,yymmddd10.);
  report_day="&report_day";
  if index(upcase(act_account_id),'MACQUARIE') or index(upcase(act_account_id),'UBS') then symbol=strip(compress(symbol||'F'));
  val_time=strip(strip(put(datepart(round(trans_time,1)),yymmddd10.))||'T'||strip(put(timepart(round(trans_time,1)),E8601TM.))||'Z');
  if put(upcase(trim(cur1)!!trim(cur2)),$instrument_group.)='FX Pairs' then Instrument=upcase(trim(cur1)!!trim(cur2));
    else Instrument=upcase(strip(cur1));
  instrument_full_name=put(symbol,$alpha_product_instrument_name.);
  rts22_name=put(partner_companie_name,$company_display_name.);
  rts22_lei=put(partner_companie_name,$comp_lei.);
  comp_name=put(partner_companie_name,$company_display_name.);
  comp_lei=put(partner_companie_name,$comp_lei.);
  price_notation=put(symbol,$alpha_product_price_notation.);
  branch_country=put(partner_companie_name,$emir_country.);
  index_underline=put(symbol,$EMIR13F.);
  index_underline_id=put(symbol,$EMIR9F.);
  underlying_index_name=put(symbol,$alpha_product_under_ind_name.);
  underlying_index_name_term=put(symbol,$alpha_product_under_ind_term.);
  underlying_id_type=put(symbol,$alpha_product_under_type.);
  notional_ccy1=put(symbol,$alpha_product_notional_ccy_a.);
  notional_ccy2=put(symbol,$alpha_product_notional_ccy_b.);
  underlying_id=put(symbol,$alpha_product_under_id.);
  QuoteCurrency=put(symbol,$cfh_SymbolQuoteCurrency.);
  InstrumentSubTypeId=input(put(symbol,$cfh_SymbolSubTypeId.),best12.);
  InstrumentPLCcy=put(symbol,$cfh_SymbolCurrencyCode.);
  SubType=put(symbol,$cfh_SubType.);
  if put(upcase(trim(cur1)!!trim(cur2)),$instrument_group.)='FX Pairs' then instrument=upcase(trim(cur1)!!trim(cur2));
    else instrument=upcase(strip(cur1));
  if cur1='GOLD' then cur1='XAU';
    else if cur1='SILVER' then cur1='XAG';
  cur1=upcase(put(cur1,$symbol_cash.));
  instrument=upcase(put(instrument,$symbol_cash.));
  instrument_group=put(instrument,$instrument_group_markets.);
  instrument_group_orig=put(instrument,$instrument_group_orig.);
  if instrument_group='Spread Bet' then do;
     instrument=compress(left(reverse(substr(left(reverse(instrument)),4))),'/');
     instrument=put(instrument,$symbol_cash.);
  end;
  product_id1=put(instrument,$alpha_product_id_a.);
  product_id2=put(instrument,$alpha_product_id_b.);
  if index(upcase(platform),'CLEAR_VISION')>0 then do;
     asset_class=put(InstrumentSymbol,cfh_asset_class.);
     cfi_code=put(InstrumentSymbol,cfh_cfi_code.);
  end;
  else if index(platform,'cosmos')>0 or index(platform,'mt5')>0 or index(platform,'mt4')>0 or index(platform,'acm')>0 then do;
     asset_class=strip(product_id1);
     if asset_class in ('.' 'ZZZ' ' ') then asset_class=put(instrument,$EMIR8F.);
     cfi_code=put(Instrument,EMIR11F.);
  end;
  citizenship=' ';
  passport=' ';
  id_number1=' ';
  first_name=' ';
  last_name=' ';
  birth_date=' ';
  if cur1='GOLD' then cur1='XAU';
     else if cur1='SILVER' then cur1='XAG';
  if cur2='CNH' then cur2='CNY';
     else if cur2 in ('PENCE' 'PNC') or cur2_sb in ('PENCE' 'PNC') then do;
        cur2='GBP';
        cur2_sb='GBP';
        trans_rate=trans_rate/100;
     end;
  if cur2_sb='CNH' then cur2_sb='CNY';
  if put(upcase(trim(cur1)!!trim(cur2)),$instrument_group.)='FX Pairs' then Instrument=upcase(trim(cur1)!!trim(cur2));
     else Instrument=upcase(strip(cur1));
 array tmp_array[71] $ tmp1-tmp71;
  do i=1 to dim(tmp_array);
     tmp_array{i}='';
  end;
 drop i;
 tmp1='NEWT';
 tmp2=strip(upcase(platform))||'-'||strip(action)||'-'||strip(act_pos_id);
 tmp4='LEI';
 tmp5=comp_lei;
 tmp6='TRUE';

 if report_type='MACQ' then do;
    if underlying_id_type='I' then tmp31='XOFF';
 end;
 tmp23=put(abs(volume_lot),best.);


 /* Buyer & Seller MIFIR logic integration */

 if partner_companie_name in ('Tradetech Alpha') then do;

    if platform='acm01' then do;

      if index(upcase(act_account_id),'MACQUARIE') or index(upcase(act_account_id),'UBS') then report_type='MACQ';
        else report_type='OTC';

      if offsetaccount='y' then do;
         tmp7='LEI';
         tmp11='LEI';
         if position_side='BUY' then do;
            tmp8=comp_lei;
            if other_counter_type='L' then tmp12=other_counter_id;
               else tmp12=customer_id;
         end;
         else do;
            if other_counter_type='L' then tmp8=other_counter_id;
               else tmp8=customer_id;
            tmp12=comp_lei;
         end;
      end;
      else do;
          if position_side='SELL' then do;
             tmp7='LEI';
             tmp8=comp_lei;
             if other_counter_type='L' then do;
                tmp11='LEI';
                tmp12=other_counter_id;
             end;
             else do;
                tmp11='INT';
                tmp12=customer_id;
             end;
          end;
          else do;
              if other_counter_type='L' then do;
                 tmp7='LEI';
                 tmp8=other_counter_id;
              end;
              else do;
                 tmp7='INT';
                 tmp8=customer_id;
              end;
              tmp11='LEI';
              tmp12=comp_lei;
          end;
      end;
    end;
    /* MT4/Cosmos platform for Tradetech Alpha */
    else do;
        if (effect='OPENING' and cmd=1) or (effect='CLOSING' and cmd=0) then do;
           tmp7='LEI';
           tmp8=comp_lei;
           if other_counter_type='C' then do;
              tmp11='INT';
              tmp12=customer_id;
           end;
           else do;
             tmp11='LEI';
             tmp12=other_counter_id;
           end;
        end;
        else do;
          if other_counter_type='C' then do;
             tmp7='INT';
             tmp8=customer_id;
          end;
          else do;
             tmp7='LEI';
             tmp8=other_counter_id;
          end;
          tmp11='LEI';
          tmp12=comp_lei;
        end;

    end;
 end; /*end of Tradetech Alpha*/

 else if partner_companie_name in ('cfh clearing', 'Finalto Asia') then do;

       if upcase(platform)='CLEAR_VISION' then do;
          if position_side='BUY' then do;
            tmp8=LegalEntityIdentifier_source;
            if tmp8=' ' then tmp8=CounterpartId_Source;
            tmp12=LegalEntityIdentifier_target;
            if tmp12=' ' then tmp12=CounterpartId_Target;
          end;
          else do;
            tmp8=LegalEntityIdentifier_target;
            if tmp8=' ' then tmp8=CounterpartId_Target;
            tmp12=LegalEntityIdentifier_source;
            if tmp12=' ' then tmp12=CounterpartId_Source;
          end;
          if length(tmp8)>=20 then tmp7='LEI';
             else tmp7='INT';
          if length(tmp12)>=20 then tmp11='LEI';
             else tmp11='INT';
       end;
 end;
 else do; /* Safecap/ Magnasale*/

    if partner_companie_name='Finalto Financial Services' and platform='acm01' then do;

            if offsetaccount='y' then do;
               tmp7='LEI';
               tmp11='LEI';
               if position_side='SELL' then do;
                  tmp8=comp_lei;
                  if other_counter_type='L' then tmp12=other_counter_id;
                     else tmp12=customer_id;
               end;
               else do;
                  if other_counter_type='L' then tmp8=other_counter_id;
                     else tmp8=customer_id;
                  tmp12=comp_lei;
               end;
            end;
            else do;
                if position_side='BUY' then do;
                   tmp7='LEI';
                   tmp8=comp_lei;
                   if other_counter_type='L' then do;
                      tmp11='LEI';
                      tmp12=other_counter_id;
                   end;
                   else do;
                      tmp11='INT';
                      tmp12=customer_id;
                   end;
                end;
                else do;
                    if other_counter_type='L' then do;
                       tmp7='LEI';
                       tmp8=other_counter_id;
                    end;
                    else do;
                       tmp7='INT';
                       tmp8=customer_id;
                    end;
                    tmp11='LEI';
                    tmp12=comp_lei;
                end;
            end;


    end;
    else do;

       if position_side='BUY' then do;
          if other_counter_type='C' then do;
             tmp7='INT';
             tmp8=customer_id;
          end;
          else do;
             tmp7='LEI';
             tmp8=other_counter_id;
          end;
       end;
       else do;
          tmp7='LEI';
          tmp8=comp_lei;
       end;
       if position_side='SELL' then do;
          if other_counter_type='C' then do;
             tmp11='INT';
             tmp12=customer_id;
          end;
          else do;
             tmp11='LEI';
             tmp12=other_counter_id;
          end;
       end;
       else do;
          tmp11='LEI';
          tmp12=comp_lei;
       end;

    end;

 end;

 tmp9=' ';
 tmp15='FALSE';
 tmp20=val_time;
 tmp21='DEAL';
 tmp22='UNIT';
 if platform='clear_vision' then do;
    if upcase(SubType)='CFDINDEX' then do;
      tmp27='BSPS';
      tmp29='';
    end;
    else do;
      tmp27='MNTR';
      if QuoteCurrency ne 'N/A' then tmp29=QuoteCurrency;
       else tmp29=InstrumentPLCcy;
    end;
    if tmp29=' ' then tmp29=strip(cur2);
 end;
 else do;
    if underlying_id_type='X' then do;
       tmp27='BSPS';
       tmp29=' ';
   end;
   else do;
       tmp27='MNTR';
       tmp29=strip(price_notation);
   end;
   if tmp29=' ' then tmp29=strip(cur2);
 end;

 tmp28=trans_rate;
 if tmp31^='XOFF' then tmp31='XXXX';

 array currency_fields[3] $50 Symbol Instrument InstrumentSymbol;
 array clean_currency[3] $50 Symbol_cl Instrument_cl InstrumentSymbol_cl;
 do i=1 to dim(currency_fields);
    clean_currency[i]=compress(scan(currency_fields[i],1,'.'),'/');
 end;
 if index(upcase(platform),'CLEAR') then tmp36=put(InstrumentSymbol_cl,$alpha_product_perm_id.);
   else tmp36=put(Instrument_cl,$alpha_product_perm_id.);
 tmp36=compress(tmp36);
 if tmp36='.' then tmp36=' ';

 if tmp36='21501059771' then do;
    if InstrumentSymbol not in ('ZZZ.L', 'ZZZ.L.sb') and Instrument not in ('ZZZ.L', 'ZZZ.L.sb') then do;
          tmp36=" ";
    end;
 end;
 tmp37=strip(InstrumentSymbol);
 if tmp37=' ' then tmp37=symbol;
 tmp38=asset_class;
 tmp39=strip(product_id2);
 if tmp39='' then tmp39='CD';
 tmp40=strip(cfi_code);
 tmp41=strip(notional_ccy1);
 if product_id2='SB' then do;
    tmp41=strip(account_currency);
    if tmp41='' then tmp41=strip(currency_user);
 end;
 if tmp41=' ' then tmp41=put(SourceAccountId,$cfhAccountCurrency.);
 if tmp41=' ' then tmp41=cur2;
 tmp43='1';
 tmp44=strip(underlying_id);
 tmp45=' ';
 tmp46=strip(underlying_index_name);
 if index(upcase(underlying_index_name_term),'MNTH')>0 then tmp47='MNTH';
 tmp60='CASH';
 if platform='acm01' then do;
    if executor_type='ALGO' then do;
       tmp61='ALG';
       tmp63='';
       *tmp64='ALG';
    end;
    else do;
       tmp61='INT';
       tmp63='GB';
       *tmp64=strip(executor);
    end;
 end;
 else do;
   tmp61='ALG';
   tmp63='';
   *tmp64='ALG';
 end;
 if tmp8=tmp5 then tmp64=tmp12;
   else tmp64=tmp8;
 if hedge=1 then do;
    tmp62='MARKETSHEDGINGAPP';
    tmp65='MARKETSHEDGINGAPP';
 end;
 else do;
    tmp62='MARKETSCLIENTAPP';
    tmp65='MARKETSHEDGINGAPP';
 end;
 tmp69='False';
 display_ind=put(partner_companie_name, rts22_display_ind.);
 if display_ind='1' then do;
    display_name=put(partner_companie_name,rts22_company.);
    display_lei=put(display_name,comp_lei.);
    drop display_name;
    display_country_code=put(partner_companie_name,rts22_country_prefix_code.);
    display_companie_name=put(partner_companie_name,rts22_display_name.);
 end;
 else do;
   display_count_code='other';
   display_lei='other';
   display_companie_name='other';
 end;
 file_name=compress("&html_path"!!"/"!!"&report_day"!!"_"!!"Finalto_"!!display_country_code!!"_"!!display_lei!!"_"!!display_companie_name!!"_"!!"Surveillance_Trades_RTS22");
run;

proc sort data=rts22_trades1 nodupkey; by _All_; run;

proc sort data=rts22_trades1(keep=file_name) out=rts22_trades_files nodupkey; by file_name; run;

data rts22_trades2;
 set rts22_trades_files(in=_head keep=file_name) rts22_trades1(keep=file_name tmp1-tmp71);
  if _head then do;
   count=0;
  /*1*/
   tmp1="report status";
   tmp2="transaction reference number";
   tmp3="trading venue transaction identification code";
   tmp4="executing entity identification code type";
   tmp5="executing entity edentification code";
   tmp6="mifid investment firm";
   tmp7="buyer code type";
   tmp8="buyer code";
   tmp9="buyer decision maker code type";
   tmp10="buyer decision maker code";
  /*11*/
   tmp11="seller code type";
   tmp12="seller code";
   tmp13="seller decision maker code type";
   tmp14="seller decision maker code";
   tmp15="";
   tmp15="transmission of order indicator";
   tmp16="buyer transmitting firm code type";
   tmp17="buyer transmitting firm code";
   tmp18="seller transmitting firm code type";
   tmp19="transmitting firmcode for the seller";
  /*21*/
   tmp20="trading date time";
   tmp21="trading capacity";
   tmp22="quantity type";
   tmp23="quantity";
   tmp24="quantity currency";
   tmp25="derivative notional increase/decrease";
   tmp26="no price indicator";
   tmp27="price type";
   tmp28="price";
   tmp29="price currency";
  /*31*/
   tmp30="net amount";
   tmp31="venue";
   tmp32="country of the branch membership";
   tmp33="up-front payment";
   tmp34="up-front payment currency";
   tmp35="complex trade component id";
   tmp36="PermID";
   tmp37="instrument full name";
   tmp38='assetclass';
   tmp39='contracttype';
   /*41*/
   tmp40="instrument classification code";
   tmp41="notional currency 1";
   tmp42="notional currency 2";
   tmp43="price multiplier";
   tmp44="underlying instrument code(s)";
   tmp45="underlying index code";
   tmp46="underlying index name";
   tmp47="underlying index term";
   tmp48="underlying instrument code other leg";
   tmp49="underlying index code other leg";
   /*51*/
   tmp50="underlying index name other leg";
   tmp51="underlying index term other leg";
   tmp52="option type";
   tmp53="no strike price indicator";
   tmp54="strike price type";
   tmp55="strike price";
   tmp56="strike price currency";
   tmp57="option exercise style";
   tmp58="maturity date";
   tmp59="expiry date";
   /*61*/
   tmp60="delivery type";
   tmp61="investment decision within firm code type";
   tmp62="investment decision within firm code";
   tmp63="country of the branch responsible";
   tmp64="execution within firm code type";
   tmp65="execution within firm";
   tmp66="country of the branch supervising";
   tmp67="waiver indicator(s)";
   tmp68="short selling indicator";
   tmp69="otc post-trade indicator(s)";
   /*71*/
   tmp70="commodity derivative indicator";
   tmp71="securities financing transaction indicator";
  end;
  else do;
    count=1;
  end;
run;

proc sort data=rts22_trades2; by file_name count; run;

data rts22_trades3;
set rts22_trades2;
 by file_name count;
   array char_(*) _CHARACTER_;
   do i=1 to dim(char_);
     char_(i)=translate(char_(i),"'",'"');
   end;
   fv=strip(file_name)!!".csv";
   FILE writeout FILEVAR=fv encoding='utf-8' lrecl=10000 dsd dropover;
   put tmp1-tmp71;
run;

/* Raw Data - to delete at the end */

data rts22_trades4;
set rts22_trades_files(in=_head keep=file_name)
    rts22_trades1(keep=file_name position_id1 positionkey act_pos_id act_account_id platform time order_type position_side
                      effect symbol instrument quantity requested_price filled_price slippage1 cur2 usd_rate slippage_usd slippage_volume
                      slippage_volume_usd volume_request_usd volume_filled_usd stp_time stp_price time_process_start time_process_end
                      speed_of_exec notional_amount1 notional_cur1 notional_amount2 notional_cur2 rate_cur2_cur2_sb1 pl_all customer_id
                      partner_companie_name other_counter_id report_day rts22_lei rts22_name
                      citizenship passport id_number1 first_name last_name birth_date);
 if _head then do;
    position_id1='Order Id';
    positionkey='Position';
    act_pos_id='Ticket';
    act_account_id='Login';
    platform='Platform';
    time='Time';
    report_day='Report Date';
    order_type='Type';
    position_side='Side';
    effect='Effect';
    symbol='Symbol';
    instrument='Instrument';
    quantity='Quantity';
    requested_price='Requested Price';
    filled_price='Filled Price';
    slippage1='Slippage';
    cur2='Instrument_Ccy';
    usd_rate='Usd_Rate';
    slippage_usd='Slippage_USD';
    slippage_volume='Slippage_Volume';
    slippage_volume_usd='Slippage_Volume_USD';
    volume_request_usd='Volume_Request_USD';
    volume_filled_usd='Volume_Filled_USD';
    stp_time='stp time';
    stp_price='stp price';
    rate_cur2_cur2_sb1='Convertion from Quote Currecny to SB Currency';
    time_process_start='Trader`s Request Time';
    time_process_start='Trader`s Request Time';
    time_process_end='Trader`s Fill Time';
    speed_of_exec='Speed of Execution';
    notional_amount1='Notional Amount 1';
    notional_cur1='Base Currency';
    notional_amount2='Notional Amount 2';
    notional_cur2='Quote Currency';
    pl_all='Profit $';
    customer_id='AID';
    partner_companie_name='Partner Company';
    rts22_name='Company';
    rts22_lei='Company Lei';
    branch_country='Country of Branch';
    citizenship='Citizenship';
    passport='Passport';
    id_number1='ID Number';
    first_name='First Name (latin)';
    last_name='Last Name (latin)';
    birth_date='Date of Birth';
    other_counter_type='Other Counterparty Identifier Type';
    other_counter_id='Other Counterparty Identifier ID';
    count=0;
   end;
   else count=1;
run;

proc sort data=rts22_trades4; by file_name count; run;

data rts22_trades5;
set rts22_trades4;
 array char_(*) _CHARACTER_;
  do i=1 to dim(char_);
    char_(i)=translate(char_(i),"'",'"');
  end;
  fv=strip(file_name)!!"_RAW_DATA.csv";
  FILE writeout FILEVAR=fv encoding='utf-8' lrecl=10000 dsd dropover;
  put position_id1 positionkey act_pos_id act_account_id platform time order_type position_side effect symbol instrument
      quantity requested_price filled_price slippage1 cur2 usd_rate slippage_usd slippage_volume slippage_volume_usd
      volume_request_usd volume_filled_usd stp_time stp_price time_process_start time_process_end speed_of_exec
      notional_amount1 notional_cur1 notional_amount2 notional_cur2 rate_cur2_cur2_sb1 pl_all customer_id
      partner_companie_name other_counter_type other_counter_id;
run;

data _null_;
set rts22_trades_files(in=_a keep=file_name) rts22_trades_files(keep=file_name) ;
 length file_name1 $200;
  if _a then file_name1=strip(compress(file_name||'_RAW_DATA'));
     else file_name1=file_name;
  zipcommand="zip -j "!!strip(file_name1)!!".zip "!!strip(file_name1)!!".csv";
  rc=system(zipcommand);
  put zipcommand=;
  put rc=;
  rc=system("cp "!!strip(file_name1)!!".zip /mnt/netapp/SAS/regulation/RTS22/");
  put rc=;
  rc=system("find /mnt/netapp/SAS/regulation/RTS22/ -mindepth 1 -mtime +14 -delete");
  put rc=;
run;