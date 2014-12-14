open "data";
.
index TransactionHistory.TransactionID;
.
index TransactionHistory.ProductID;
.
index WorkOrderRouting.WorkOrderID;
.
index WorkOrderRouting.ProductID;
.
select * from TransactionHistory;
.
select * from WorkOrderRouting;
.
select * from TransactionHistory,WorkOrderRouting where TransactionHistory.ProductID=WorkOrderRouting.ProductID and TransactionHistory.ProductID="931";
.
commit;
.
close;
.
exit;
.
