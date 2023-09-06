select count(c_id) as count_c_id from customer where c_w_id=1 and c_d_id=1 and c_last='Zhang';
select c_balance, c_first, c_middle, c_last from customer where c_w_id=1 and c_d_id=1 and c_last='Zhang' order by c_fisrt;
select o_id, o_entry_d, o_carrier_id from orders where o_w_id=1 and o_d_id=1 and o_c_id=1 and o_id=1;
select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where ol_w_id=1 and ol_d_id=1 and ol_o_id=1;