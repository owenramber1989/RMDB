select min(no_o_id) as min_o_id from new_orders where no_d_id=1 and no_w_id=1;
delete from new_orders where no_o_id=1 and no_d_id=1;
select o_c_id from orders where o_id=1 and o_d_id=1 and o_w_id=1;
update orders set o_carrier_id=1 where o_id=1 and o_d_id=1 and o_w_id=1;
update order_line set ol_delivery_d='2023-08-19 12:12:12' where ol_o_id=1 and ol_d_id=1 and ol_w_id=1;
select sum(ol_amount) as sum_amount from order_line where ol_o_id=1 and ol_d_id=1;
update customer set c_balance=1, c_delivery_cnt=1 where c_id=1 and c_d_id=1 and c_w_id=1;