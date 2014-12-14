open "data";
.
delete  WorkOrderRouting where ProductID="931";
.
commit;
.
select * from TransactionHistory,WorkOrderRouting where TransactionHistory.ProductID=WorkOrderRouting.ProductID and TransactionHistory.ProductID="931";
.
close;
.
exit;
.
