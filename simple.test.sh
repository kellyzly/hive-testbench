cd sample-queries-tpcds;
#export HADOOP_USER_CLASSPATH_FIRST=true;
#dpp=false
settings=testbench.settings
for arg in "$@";do
     echo "arg:"$arg
     case $arg in
      --query*)
        query=$(echo $arg | cut -d"=" -f2)
      ;;  
      --database*)
        database=$(echo $arg | cut -d"=" -f2)
      ;;  
      --settings*)
        settings=$(echo $arg | cut -d"=" -f2)
      ;;  
      --debug*)
        debug=$(echo $arg | cut -d"=" -f2)
      ;;  
      esac
done
queryfile=query$query.sql;
debug_script=""
if [ "$debug" == "true" ];then
    debug_script="--debug"

fi
echo "use $database;source $queryfile;"|hive $debug_script --hiveconf spark.app.name=$queryfile -i $settings
