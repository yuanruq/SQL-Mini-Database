open "don";
.
create table emp ( empno int,
              age   int,
              dept_name str,
              name str
            );
.
create table dept ( deptno int,
               dept_name str,
               chairman str
             );
.
create table univ( univno int,
					univ_name str,
					empno int,
					deptno int);
.

show;
.
script "/Users/bansal/Desktop/DB/bansal/cs386_P4/test/test2.dml";
.
close;
.
exit;
.
