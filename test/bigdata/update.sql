open "data";
.
update WorkOrderRouting set ProductID="931" where ProductID>"931" and ProductID<"950" and WorkOrderID<"11814";
.
select * from WorkOrderRouting where ProductID="931";
.
commit;
.
close;
.
exit;
.
