/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1305283675
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): using percentile-filtered average is the best approach, so far
*/

/* using percentile-filtered average */

-- merchants who have been active(event+txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, event_date, date_sequence
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

-- merchants who have been active(only txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, created_datetime, date_sequence
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

-- merchant-wise, outlier-free ranges of gaps between consecutive active (event+txn) days
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	mobile_no, 
	-- change
	percentile_cont(0.15) within group(order by gap_consec_days asc) pct_lim1, 
	percentile_cont(0.85) within group(order by gap_consec_days asc) pct_lim2
from 
	(-- gaps (in days) between consecutive days of activity
	select tbl1.mobile_no, tbl1.event_date, tbl1.date_sequence, tbl2.event_date, tbl2.date_sequence, tbl2.event_date-tbl1.event_date gap_consec_days
	from 
		data_vajapora.help_a tbl1 
		inner join 
		data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	) tbl1
group by 1; 
			
-- merchant-wise, outlier-free ranges of gaps between consecutive active (only txn) days
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	mobile_no, 
	-- change
	percentile_cont(0.15) within group(order by gap_consec_days asc) pct_lim1, 
	percentile_cont(0.85) within group(order by gap_consec_days asc) pct_lim2
from 
	(-- gaps (in days) between consecutive days of activity
	select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
	from 
		data_vajapora.help_b tbl1 
		inner join 
		data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	) tbl1
group by 1; 

-- see the distribution 
select *
from 
	(-- distribution of merchants according to gaps between consecutive active (event+txn) days 
	select avg_gap_consec_days, count(mobile_no) event_plus_txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select tbl1.mobile_no, mode() within group(order by gap_consec_days) avg_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.event_date, tbl1.date_sequence, tbl2.event_date, tbl2.date_sequence, tbl2.event_date-tbl1.event_date gap_consec_days
			from 
				data_vajapora.help_a tbl1 
				inner join 
				data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
			inner join 
			data_vajapora.help_c tbl2 on(tbl1.mobile_no=tbl2.mobile_no and gap_consec_days>=pct_lim1 and gap_consec_days<=pct_lim2)
		group by 1
		) tbl1
	group by 1
	) tbl1 
	
	inner join 
	
	(-- distribution of merchants according to gaps between consecutive active (only txn) days 
	select avg_gap_consec_days, count(mobile_no) txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select tbl1.mobile_no, mode() within group(order by gap_consec_days) avg_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
			from 
				data_vajapora.help_b tbl1 
				inner join 
				data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
			inner join 
			data_vajapora.help_d tbl2 on(tbl1.mobile_no=tbl2.mobile_no and gap_consec_days>=pct_lim1 and gap_consec_days<=pct_lim2)
		group by 1
		) tbl1
	group by 1
	) tbl2 using(avg_gap_consec_days)
order by 1; 

-- sanity checks
select *
from 
	(-- random user(s) from a particular frequency 
	select tbl1.mobile_no, mode() within group(order by gap_consec_days) avg_gap_consec_days
	from 
		(-- gaps (in days) between consecutive days of activity
		select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
		from 
			data_vajapora.help_b tbl1 
			inner join 
			data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
		) tbl1
		inner join 
		data_vajapora.help_d tbl2 on(tbl1.mobile_no=tbl2.mobile_no and gap_consec_days>=pct_lim1 and gap_consec_days<=pct_lim2)
	group by 1
	having mode() within group(order by gap_consec_days)=3 -- change
	order by random() 
	limit 20
	) tbl1
	
	inner join 
	
	(-- gaps (in days) between consecutive days of activity
	select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
	from 
		data_vajapora.help_b tbl1 
		inner join 
		data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	) tbl2 using(mobile_no); 



/* using average */

-- merchants who have been active(event+txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, event_date, date_sequence
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

-- merchants who have been active(only txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, created_datetime, date_sequence
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

select *
from 
	(-- distribution of merchants according to gaps between consecutive active (event+txn) days 
	select avg_gap_consec_days, count(mobile_no) event_plus_txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select mobile_no, round(avg(gap_consec_days)) avg_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.event_date, tbl1.date_sequence, tbl2.event_date, tbl2.date_sequence, tbl2.event_date-tbl1.event_date gap_consec_days
			from 
				data_vajapora.help_a tbl1 
				inner join 
				data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
		group by 1
		) tbl1
	group by 1
	) tbl1 
	
	inner join 
	
	(-- distribution of merchants according to gaps between consecutive active (only txn) days 
	select avg_gap_consec_days, count(mobile_no) txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select mobile_no, round(avg(gap_consec_days)) avg_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
			from 
				data_vajapora.help_b tbl1 
				inner join 
				data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
		group by 1
		) tbl1
	group by 1
	) tbl2 using(avg_gap_consec_days)
order by 1; 

-- sanity checks
select *
from 
	(-- random user(s) from a particular frequency 
	select mobile_no, round(avg(gap_consec_days)) avg_gap_consec_days
	from 
		(-- gaps (in days) between consecutive days of activity
		select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
		from 
			data_vajapora.help_b tbl1 
			inner join 
			data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
		) tbl1
	group by 1
	having round(avg(gap_consec_days))=7
	order by random() 
	limit 20
	) tbl1
	
	inner join 
	
	(-- gaps (in days) between consecutive days of activity
	select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
	from 
		data_vajapora.help_b tbl1 
		inner join 
		data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	) tbl2 using(mobile_no); 



/* using mode */

-- merchants who have been active(event+txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, event_date, date_sequence
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_user_date_sequence_final
	where event_date>=current_date-90 and event_date<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

-- merchants who have been active(only txn)>=5 days in the last 90 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, created_datetime, date_sequence
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	) tbl1 
	
	inner join 
	
	(select mobile_no, count(date_sequence) days_active_last_90_days
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-90 and created_datetime<current_date
	group by 1
	having count(date_sequence)>=5 -- change
	) tbl2 using(mobile_no);

select *
from 
	(-- distribution of merchants according to gaps between consecutive active (event+txn) days 
	select modal_gap_consec_days, count(mobile_no) event_plus_txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select mobile_no, mode() within group(order by gap_consec_days) modal_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.event_date, tbl1.date_sequence, tbl2.event_date, tbl2.date_sequence, tbl2.event_date-tbl1.event_date gap_consec_days
			from 
				data_vajapora.help_a tbl1 
				inner join 
				data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
		group by 1
		) tbl1
	group by 1
	) tbl1 
	
	inner join 
	
	(-- distribution of merchants according to gaps between consecutive active (only txn) days 
	select modal_gap_consec_days, count(mobile_no) txn_merchants
	from 
		(-- merchant-wise mode of gaps between consecutive active days 
		select mobile_no, mode() within group(order by gap_consec_days) modal_gap_consec_days
		from 
			(-- gaps (in days) between consecutive days of activity
			select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
			from 
				data_vajapora.help_b tbl1 
				inner join 
				data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
			) tbl1
		group by 1
		) tbl1
	group by 1
	) tbl2 using(modal_gap_consec_days)
order by 1; 

-- sanity checks
select *
from 
	(-- random user(s) from a particular frequency 
	select mobile_no, mode() within group(order by gap_consec_days) modal_gap_consec_days
	from 
		(-- gaps (in days) between consecutive days of activity
		select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
		from 
			data_vajapora.help_b tbl1 
			inner join 
			data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
		) tbl1
	group by 1
	having mode() within group(order by gap_consec_days)=7
	order by random() 
	limit 5
	) tbl1
	
	inner join 
	
	(-- gaps (in days) between consecutive days of activity
	select tbl1.mobile_no, tbl1.created_datetime, tbl1.date_sequence, tbl2.created_datetime, tbl2.date_sequence, tbl2.created_datetime-tbl1.created_datetime gap_consec_days
	from 
		data_vajapora.help_b tbl1 
		inner join 
		data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	) tbl2 using(mobile_no); 
