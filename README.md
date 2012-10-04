parse_binlog.pl
===============

 parse INSERT, UPDATE, DELETE event from row-formated binlog 
 #! /usr/bin/perl -w
#
#  parse INSERT, UPDATE, DELETE event from row-formated binlog
#  usage: mysqlbinlog --no-defaults -vvv <binlog> | parse_binlog > out.sql
#  junda@alipay.com, 2012-04-28  
#  use it at your own risk.
#
my $state="init";
my $i=0;
my $col=0;
my $tabname;

my $sql;

my $var;
my $type;

while (<>) {
    chomp;
    if ($state eq "init") {
        if ($_ =~ m/^### UPDATE ([a-zA-Z._0-9]+)/) {
             $tabname=$1;
             print "--table: $1\n";
             $state="upd";
             $type = "UPDATE";
        }
        elsif ($_ =~ m/^### INSERT INTO ([a-zA-Z._0-9]+)/) {
             $tabname=$1;
             print "--table: $1\n";
             $state="ins";
             $type = "INSERT";
        } elsif ($_ =~ m/^### DELETE FROM ([a-zA-Z._0-9]+)/) {
             $tabname=$1;
             print "--table: $1\n";
             $state="del";
             $type = "DELETE";
        }
    }
    if ($state eq "upd") {
        if ($_ =~ m/^### SET/) {           
            $state = "coll";
        }
        $col = 0; 
    }
    if ($state eq "ins") {
        if ($_ =~ m/^### SET/) {           
            $state = "coll";
        }
        $col = 0; 
    }
    if ($state eq "del") {
        if ($_ =~ m/^### WHERE/) {           
            $state = "coll";
        }
        $col = 0; 
    }
    if ($state eq "coll") {
       if ($_ =~ m/^###   @([0-9]?[0-9])=(.*) +\/[*].*[*]\/$/ ) {
           #print "$1, $2\n";
           # 2012-04-21 12:43:22
           $var = $2;
           if ($var =~ m/([0-9][0-9][01][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])/) {
               $var = "'$1'";
           }
           if ($var =~ m/(-[0-9]+) \([0-9]+\)/) {
               $var = $1;
           }
           if ($col == 0) {
              $sql = "replace into $tabname values ( $var ";
           } else {  
              $sql = $sql . ", $var";
           }
           $col++;
       } elsif ($_ =~ m/^# at [0-9]+$/) {
           $state="init";
           $sql = $sql . ");";
           print "--DML type: $type, num of cols: $col\n$sql\n";
       } elsif ($_ =~ m/^### UPDATE ([a-zA-Z._0-9]+)/) {
           $sql = $sql . ");";
           print "--DML type: $type, num of cols: $col\n$sql\n";
           $tabname=$1;
           $state="upd";
       } elsif ($_ =~ m/^### INSERT INTO ([a-zA-Z._0-9]+)/) {
           $sql = $sql . ");";
           print "--DML type: $type, num of cols: $col\n$sql\n";
           $tabname=$1;
           $state="ins";
       } elsif ($_ =~ m/^### DELETE FROM ([a-zA-Z._0-9]+)/) {
           $sql = $sql . ");";
           print "--DML type: $type, num of cols: $col\n$sql\n";
           $tabname=$1;
           $state="del";
       }
    }

    #print "line $i: $state\n";
    $i++;
}

mysql> USE T1
Database changed
mysql> delete from t1 where id<12;
Query OK, 2 rows affected (0.00 sec)

mysqlbinlog -vvv /home/mysql/data3006/mysql/mysql-bin.000004 |/root/parse_binlog.pl >/tmp/parse.sql1
more /tmp/parse/sql1
--DML type: DELETE, num of cols: 2
replace into t1.t1 values ( 10 , 'ni hao1');
--DML type: DELETE, num of cols: 2
replace into t1.t1 values ( 11 , 'ni hao1');
