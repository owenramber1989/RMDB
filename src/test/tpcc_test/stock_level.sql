select d_next_o_id from district where d_id=1 and d_w_id=1;
select ol_i_id from order_line where ol_w_id=1 and ol_d_id=1 and ol_o_id<3000 and ol_o_id>=2080;
select count(*) as count_stock from stock where s_w_id=1 and s_i_id=1 and s_quantity<1;