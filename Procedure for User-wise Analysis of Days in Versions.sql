CREATE OR REPLACE FUNCTION data_vajapora.fn_version_wise_days()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of - how many days users are spending in each version of TK, users' latest versions
Auxiliary data table(s) : data_vajapora.help_version, data_vajapora.help_version_2
Target data table(s)    : data_vajapora.version_wise_days, tallykhata.tk_user_app_version
*/

declare

begin
	
	-- bringing version-numbers against each version
	drop table if exists data_vajapora.help_version_2; 
	create table data_vajapora.help_version_2 as
	select app_version_name, max(app_version_number) app_version_number
	from public.register_historicalregistereduser 
	group by 1; 
	
	-- validly sequencing versions against each user
	drop table if exists data_vajapora.help_version; 
	create table data_vajapora.help_version as
	select *, row_number() over(partition by mobile_no order by update_datetime asc, app_version_number asc) version_seq
	from 
		(select mobile mobile_no, app_version_name, min(history_date) update_datetime
		from public.register_historicalregistereduser 
		group by 1, 2
		) tbl1
		inner join 
		data_vajapora.help_version_2 tbl2 using(app_version_name); 
	
	-- counting number of days spent in different versions
	drop table if exists data_vajapora.version_wise_days; 
	create table data_vajapora.version_wise_days as
	select
		tbl1.mobile_no, 
		tbl1.app_version_number,
		tbl1.app_version_name,
		tbl1.update_datetime update_or_reg_datetime,
		tbl0.reg_datetime, 
		case 
			when tbl2.update_datetime is not null then date(tbl2.update_datetime)-date(tbl1.update_datetime)
			else current_date-date(tbl1.update_datetime)
		end days_in_version
	from 
		(select mobile_number mobile_no, created_at reg_datetime
		from public.register_usermobile
		) tbl0
		inner join
		data_vajapora.help_version tbl1 using(mobile_no)
		left join 
		data_vajapora.help_version tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.version_seq=tbl2.version_seq-1)
	order by 1 asc, 4 asc, 3 asc;

	-- bringing user-wise latest version 
	drop table if exists tallykhata.tk_user_app_version;
	create table tallykhata.tk_user_app_version as
	select 
		tbl1.mobile_no,
		app_version_number latest_version_number,
		app_version_name latest_version
	from 
		(select mobile_no, max(update_or_reg_datetime) max_date
		from data_vajapora.version_wise_days
		group by 1
		) tbl1
		inner join 
		(select mobile_no, update_or_reg_datetime, app_version_name, app_version_number
		from data_vajapora.version_wise_days
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.max_date=tbl2.update_or_reg_datetime);

	-- dropping auxiliary tables
	drop table if exists data_vajapora.help_version; 
	drop table if exists data_vajapora.help_version_2; 

END;
$function$
;
