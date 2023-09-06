update warehouse set w_ytd=w_ytd+1.0 where w_id=1;
select w_street_1, w_street_2, w_city, w_state, w_zip, w_name from warehouse where w_id=1;
update district set d_ytd=d_ytd+1.0 where d_w_id=1 and d_id=1;
select d_street_1, d_street_2, d_city, d_state, d_zip, d_name from district where d_w_id=1 and d_id=1;
select c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since from customer where c_w_id=1 and c_d_id=1 and c_id=1;
update customer set c_balance=1.0 where c_w_id=1 and c_d_id=1 and c_id=1;
insert into history values(1, 1, 1, 1, 1, '2023-10-10 12:12:10',1.0, 'fdsfdSAf');