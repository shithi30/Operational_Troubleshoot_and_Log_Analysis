select date(tbl2.app_status_created_at) uninstall_to_active_date, count(tbl2.device_id) devices_reinstalled
from 
	(select *, row_number() over(partition by device_id order by app_status_created_at asc) seq
	from
		(select device_id, created_at app_status_created_at, app_status
		from sync_operation.notification_fcmtoken 
		) tbl1 
	) tbl1
	
	inner join 
	
	(select *, row_number() over(partition by device_id order by app_status_created_at asc) seq
	from
		(select device_id, created_at app_status_created_at, app_status
		from sync_operation.notification_fcmtoken 
		) tbl1 
	) tbl2 on(tbl1.device_id=tbl2.device_id and tbl1.seq=tbl2.seq-1)
where tbl1.app_status='UNINSTALLED' and tbl2.app_status='ACTIVE'
group by 1
order by 1; 