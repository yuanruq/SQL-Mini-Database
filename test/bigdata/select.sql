open "data";
.
select * from TransactionHistory;
.
select * from WorkOrderRouting;
.
select * from WorkOrderRouting where ProductID="931";
.
select * from TransactionHistory,WorkOrderRouting where TransactionHistory.ProductID=WorkOrderRouting.ProductID and TransactionHistory.ProductID="931"and WorkOrderRouting.ProductID="931";
.
close;
.
exit;
.
