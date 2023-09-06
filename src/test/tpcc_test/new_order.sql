select c_discount, c_last, c_credit, w_tax from customer, warehouse where w_id=1 and c_w_id=1 and c_d_id=1 and c_id=1;
select d_next_o_id, d_tax from district where d_id=1 and d_w_id=1;
update district set d_next_o_id=d_next_o_id+1 where d_id=1 and d_w_id=1;
insert into orders values(1, 1, 1, 1, '2023-08-09 11:11:11', 1,3, 1);
insert into new_orders values(2, 1, 1);
select i_price, i_name, i_data from item where i_id=1;
select s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 from stock where s_i_id=1 and s_w_id = 1;
UPDATE stock SET s_quantity=1 where s_i_id = 1 and s_w_id = 1
insert into order_line values (1, 1, 1, 1, 1,1, '2023-08-19 12:12:12', 1, 1.0, '21212JfdA9');