open "ak";
.
script "/Users/yuanruqian/Desktop/test3.dml";
.

select * from products_sale where sold>5 and buyerid<=2500;
.
select buyer_name from buyer where buyer_id=2673;
.
select p_name from products where p_id=1;
.
update products set p_name="bbb" where p_id=19135;
.
update buyer set buyer_name="abc" where buyerid>2500;
.
abort;
.
delete products_sale where buyerid=2029;
.
show;
.
close;
.
exit;
.
