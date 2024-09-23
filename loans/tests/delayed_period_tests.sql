-- Delayed period in orders not in arrears should be always equal to zero
select 
    *
from int_orders_not_in_arrears
where delayed_period <> 0