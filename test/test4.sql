open "don";
.
script "/Users/yuanruqian/Documents/JavaProject/SQL Mini Database/test/test4.dml";
.
select * from products_sale where sold>5 and buyerid<=2500;
.
select buyer_name from buyer where buyer_id=2673;
.
select p_name from products where p_id=1;
.
update products set p_name="aaa" where p_id=19135;
.
select * from products where p_id=19135;
.
commit;
.
update products set p_name="bbb" where p_id=19135;
.
select * from products where p_id=19135;
.
abort;
.
delete products_sale where buyerid=2029;
.
commit;
.
select p_name,buyer_name,sold
from products,buyer,products_sale
where p_id=pid and buyer_id=buyerid and buyerid=2001;
.
show;
.
close;
.
open "bansal";
.
script "/Users/bansal/Desktop/DB/bansal/cs386_P4/test/test4.dml";
.
select * from products_sale where sold>5 and buyerid<=2500;
.
select buyer_name from buyer where buyer_id=2673;
.
show;
.
close;
.
exit;
.
