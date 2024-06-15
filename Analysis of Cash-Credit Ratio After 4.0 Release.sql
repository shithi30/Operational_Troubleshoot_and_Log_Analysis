-- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=713774009

-- credit+cash TRV after 4.0 release
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no, 
	case when cash_trv=0 then 0.00000001 else cash_trv end cash_trv, -- replace 0s with very small numbers with no impact on ratios
	case when credit_trv=0 then 0.00000001 else credit_trv end credit_trv
from 
	(select 
		mobile_no, 
		sum(case when txn_type in ('MALIK_NILO', 'MALIK_DILO', 'EXPENSE', 'CASH_PURCHASE', 'CASH_SALE', 'CASH_ADJUSTMENT') then cleaned_amount else 0 end) cash_trv,
		sum(case when txn_type in ('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE') then cleaned_amount else 0 end) credit_trv
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>='2021-09-29'
	group by 1
	) tbl1
where not(cash_trv=0 and credit_trv=0); -- exclude merchants who did neither cash nor credit txns 

-- distribution of credit vs. cash TRV pct, after 4.0 release
select 
	sum(cash_trv) cash_trv,
	sum(credit_trv) credit_trv,
	sum(cash_trv)/(sum(cash_trv)+sum(credit_trv)) cash_trv_pct,
	sum(credit_trv)/(sum(cash_trv)+sum(credit_trv)) credit_trv_pct
from data_vajapora.help_a; 

-- distribution of cash to credit ratio after 4.0 release 
select 
	count(mobile_no) "cash+credit users after 4.0 release", 
	count(case when cash_to_credit_trv_ratio>=0 and cash_to_credit_trv_ratio<0.01 then mobile_no else null end) "cash to credit TRV ratio: 0%-1%",
	count(case when cash_to_credit_trv_ratio>=0.01 and cash_to_credit_trv_ratio<0.1 then mobile_no else null end) "cash to credit TRV ratio: 1%-10%",
	count(case when cash_to_credit_trv_ratio>=0.1 and cash_to_credit_trv_ratio<0.2 then mobile_no else null end) "cash to credit TRV ratio: 10%-20%",
	count(case when cash_to_credit_trv_ratio>=0.2 and cash_to_credit_trv_ratio<0.3 then mobile_no else null end) "cash to credit TRV ratio: 20%-30%",
	count(case when cash_to_credit_trv_ratio>=0.3 and cash_to_credit_trv_ratio<0.4 then mobile_no else null end) "cash to credit TRV ratio: 30%-40%",
	count(case when cash_to_credit_trv_ratio>=0.4 and cash_to_credit_trv_ratio<0.5 then mobile_no else null end) "cash to credit TRV ratio: 40%-50%",
	count(case when cash_to_credit_trv_ratio>=0.5 and cash_to_credit_trv_ratio<0.6 then mobile_no else null end) "cash to credit TRV ratio: 50%-60%",
	count(case when cash_to_credit_trv_ratio>=0.6 and cash_to_credit_trv_ratio<0.7 then mobile_no else null end) "cash to credit TRV ratio: 60%-70%",
	count(case when cash_to_credit_trv_ratio>=0.7 and cash_to_credit_trv_ratio<0.8 then mobile_no else null end) "cash to credit TRV ratio: 70%-80%",
	count(case when cash_to_credit_trv_ratio>=0.8 and cash_to_credit_trv_ratio<0.9 then mobile_no else null end) "cash to credit TRV ratio: 80%-90%",
	count(case when cash_to_credit_trv_ratio>=0.9 and cash_to_credit_trv_ratio<=1.0 then mobile_no else null end) "cash to credit TRV ratio: 90%-100%",
	count(case when cash_to_credit_trv_ratio>1.0 then mobile_no else null end) "cash to credit ratio > 100%"
from 
	(select *, cash_trv*1.00/credit_trv cash_to_credit_trv_ratio
	from data_vajapora.help_a
	) tbl1; 

-- distribution of cash TRV after 4.0 release
select
	count(mobile_no) "cash users after 4.0 release",
	count(case when cash_trv>=0 and cash_trv<1000 then mobile_no else null end) "cash TRV 0k-1k",
	count(case when cash_trv>=1000 and cash_trv<5000 then mobile_no else null end) "cash TRV 1k-5k",
	count(case when cash_trv>=5000 and cash_trv<10000 then mobile_no else null end) "cash TRV 5k-10k",
	count(case when cash_trv>=10000 and cash_trv<25000 then mobile_no else null end) "cash TRV 10k-25k",
	count(case when cash_trv>=25000 and cash_trv<40000 then mobile_no else null end) "cash TRV 25k-40k",
	count(case when cash_trv>=40000 and cash_trv<60000 then mobile_no else null end) "cash TRV 40k-60k",
	count(case when cash_trv>=60000 and cash_trv<100000 then mobile_no else null end) "cash TRV 60k-100k",
	count(case when cash_trv>=100000 and cash_trv<120000 then mobile_no else null end) "cash TRV 100k-120k",
	count(case when cash_trv>=120000 and cash_trv<150000 then mobile_no else null end) "cash TRV 120k-150k",
	count(case when cash_trv>=150000 and cash_trv<180000 then mobile_no else null end) "cash TRV 150k-180k",
	count(case when cash_trv>=180000 and cash_trv<=200000 then mobile_no else null end) "cash TRV 180k-200k",
	count(case when cash_trv>200000 then mobile_no else null end) "cash TRV > 200k"
from data_vajapora.help_a; 

