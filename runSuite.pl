#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use Time::Piece;
# PROTOTYPES
sub dieWithUsage(;$);

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );
my $logfile = 'log.ds';
my $outstream;
open $outstream, '>>', $logfile;

# MAIN
dieWithUsage("one or more parameters not defined") unless @ARGV >= 1;
my $suite = shift;
my $scale = shift || 2;
dieWithUsage("suite name required") unless $suite eq "tpcds" or $suite eq "tpch";

chdir $SCRIPT_PATH;
if( $suite eq 'tpcds' ) {
	chdir "sample-queries-tpcds";
} else {
	chdir 'sample-queries-tpch';
} # end if
my @queries = glob '*.sql';

my $db = { 
	'tpcds' => "tpcds_bin_partitioned_orc_$scale",
	'tpch' => "tpch_flat_orc_$scale"
};
my $starttime = localtime->strftime('%Y/%m/%d %H:%M:%S');
print { $outstream } "start time:$starttime\n"; 
print { $outstream } "filename,status,time,rows\n";
for my $query ( @queries ) {
	my $logname = "$query.log";
	my $cmd="echo 'use $db->{${suite}}; source $query;' | hive -i testbench.settings --hiveconf spark.app.name=$query 2>&1  | tee $query.log";
#	my $cmd="cat $query.log";
	#print $cmd ; exit;
	
	my $hiveStart = time();

	my @hiveoutput=`$cmd`;
	die "${SCRIPT_NAME}:: ERROR:  hive command unexpectedly exited \$? = '$?', \$! = '$!'" if $?;

	my $hiveEnd = time();
	my $hiveTime = $hiveEnd - $hiveStart;
	foreach my $line ( @hiveoutput ) {
		if( $line =~ /Time taken:\s+([\d\.]+)\s+seconds,\s+Fetched:\s+(\d+)\s+row/ ) {
			print { $outstream } "$query,success,$hiveTime,$2\n"; 
		} elsif( 
			$line =~ /^FAILED: /
			# || /Task failed!/ 
			) {
			print { $outstream } "$query,failed,$hiveTime\n"; 
		} # end if
	} # end while
} # end for

my $endtime = localtime->strftime('%Y/%m/%d %H:%M:%S');
print { $outstream } "end time:$endtime\n"; 
close $outstream;

sub dieWithUsage(;$) {
	my $err = shift || '';
	if( $err ne '' ) {
		chomp $err;
		$err = "ERROR: $err\n\n";
	} # end if

	print STDERR <<USAGE;
${err}Usage:
	perl ${SCRIPT_NAME} [tpcds|tpch] [scale]

Description:
	This script runs the sample queries and outputs a CSV file of the time it took each query to run.  Also, all hive output is kept as a log file named 'queryXX.sql.log' for each query file of the form 'queryXX.sql'. Defaults to scale of 2.
USAGE
	exit 1;
}

