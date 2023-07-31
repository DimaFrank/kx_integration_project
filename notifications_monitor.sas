
options mprint;

%let today=%sysfunc(putn(%sysfunc(today()),ddmmyy10.));
%put today=&today;  

%let path1=/home/sas/projects/trade/prog;
%let path2=/home/sas/projects/cfh/cfhregsql;

filename lsinfo1 pipe "ls --full-time &path1/*";
filename lsinfo2 pipe "ls --full-time &path2/*";

%let prod_files="emir_trades.sas" "emir_collateral.sas" "mifir_trades.sas" "reg_rep1_emir.sas" "reg_rep1_mifir.sas";
%put prod_files=&prod_files;

 data file_list1;
   infile lsinfo1 pad;
    input ls_string $250.;
 run;

 data file_list2;
   infile lsinfo2 pad;
    input ls_string $250.;
 run;

 proc format;
   value $ reg_output_files
     "emir_trades.sas"="EMIR_TradeExport / EMIR_OpenPositions"
     "emir_collateral.sas"="Collateral"
     "mifir_trades.sas"="MIFIR"
     "reg_rep1_emir.sas"="FFS_EMIR_TradeExport / FFS_EMIR_OpenPostions"
     "reg_rep1_mifir.sas"="MIFIR_CFH";
 run;

 data full_file_list;
   length program_name file_path report comment_ $250;
   set file_list1(in=_a) file_list2(in=_b);
   program_name=scan(ls_string,-1,'/');
   file_path=scan(ls_string,-1," ");
   date_modified=scan(ls_string,6," ");
   time_modified=scan(scan(ls_string,7," "),1,'.');
   date_time=input(catx(" ",date_modified,time_modified),anydtdtm.);
   report=put(program_name,reg_output_files.);
   comment_=" ";
   if program_name in (&prod_files);
   format date_time datetime.;
   drop date_modified time_modified ls_string;
   rename date_time=last_update;
 run;

 proc sql;
   create table delta as
    select a.*, b.last_update as last_update_new
     from trade.notifications_monitor as a
      left join full_file_list as b
       on a.program_name=b.program_name
     where a.last_update^=b.last_update;
 quit;

 %let delta_n_obs=&SYSNOBS;
 %put delta_n_obs=&delta_n_obs;

 proc sql;
  select program_name, report, last_update_new, comment_, entity
   into  :prog_name separated by ",",
         :report    separated by " ",
         :f_update  separated by ",",
         :comm      separated by " ",
         :entity    separated by ","
   from delta;
 quit;



 %macro notification_create;

    %if %eval(&delta_n_obs > 0) %then %do;

        /* Email Notification sending */

        %let emails="email1@finalto.com" "email2y@finalto.com"  "email3@finalto.com";

        filename outmail email
                         to=(&emails)
                         subject=("&prog_name was updated, &today")
                         cc=("email4@finalto.com")
                         ;

        data _null_;
          file outmail;
          put "Please Note: &report report was affected by updating the program &prog_name";
          put;
          put "Last update: &f_update";
          put;
          put "Entity: &entity";
          put;
          put "Comment: &comm";
        run;


        /* Monitor dataset update */
        proc sql;
           update trade.notifications_monitor as t1
            set last_update=(select last_update from full_file_list as t2 where t1.program_name=t2.program_name);
        quit;

        proc sort data=trade.notifications_monitor; by descending last_update; run;

        %symdel delta_n_obs;

    %end;

 %mend;


 %notification_create;