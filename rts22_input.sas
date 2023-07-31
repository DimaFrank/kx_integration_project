
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


%macro set_orders;

   %if &weekday >=2 and &weekday <=6 %then %do;

      %if %eval(&weekday=2) %then %let rep_cfh_date=%eval(&today-3);
         %else %let rep_cfh_date=%eval(&today-1);

      %let rep_cfh_date=%sysfunc(putn(&rep_cfh_date,yymmddn8.));
      %put rep_cfh_date=&rep_cfh_date;

      %let orders_file="&path_to_raw_file./Daily&rep_cfh_date..csv";
      %put orders_file=&orders_file;

      %if %sysfunc(fileexist(&orders_file)) %then %do;

         data cfh.rts24_journal_&rep_cfh_date;

             infile &orders_file dsd firstobs=2 truncover;

             length Time $50 OrderId AccountId ClientOrderId $50 OrderType Side InstrumentId 8. Amount Price $50 Duration 8.
                    ExpiryDate OrderTime $20 Filled AveragePrice $50 TriggerSide PriceLevel2 ContingentType RelatedOrder1 RelatedOrder2 8.
                    RelatedToPosition $50 Track StopDistanceIfFilled TakeProfitDistanceIfFilled 8. AppId ExtClientId $50
                    Trail StopTrailDistanceIfFilled 8. SourceOrderId $20 OrderState 8. Gateway MinQty MatchIncrement $50 TradeSystemId 8.
                    ;
             input Time OrderId AccountId ClientOrderId OrderType Side InstrumentId Amount Price Duration ExpiryDate OrderTime
                   Filled AveragePrice TriggerSide PriceLevel2 ContingentType RelatedOrder1 RelatedOrder2 RelatedToPosition
                   Track StopDistanceIfFilled TakeProfitDistanceIfFilled AppId ExtClientId Trail StopTrailDistanceIfFilled
                   SourceOrderId OrderState Gateway MinQty MatchIncrement TradeSystemId;

             Time1=input(Time,ANYDTDTM.);
             ExpiryDate1=input(ExpiryDate,ANYDTDTM.);
             OrderTime1=input(OrderTime,ANYDTDTM.);

             format Time1 ExpiryDate1 OrderTime1 datetime20.;

             drop Time ExpiryDate OrderTime;

         run;

         proc format lib=cfh;
          value rts24_duration_to_validityPeriod
           0="IOCV"
           1="FOKV"
           2="DAVY"
           3="GTDV"
           4="GTCV";

          value rts24_orderstate_to_orderevent
          -1="UNKN"
           0="NEWO"
           1="NEWO"
           2="REMO"
           3="FILL"
           4="TRIG"
           5="CAMO";

          value rts24_ordertype
           0="Market"
           1="Stop"
           2="Limit"
           3="StopLimit"
           4="Liquidation";
         run;

      %end;

   %end;

%mend;

%set_orders;