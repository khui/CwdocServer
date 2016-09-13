#!/usr/bin/env bash
timestamp=`date +"%m%d%M%H"`
id=$timestamp.`hostname`.$BASHPID
echo $id

javaworkspace=/home/khui/workspace/IdeaProjects/docsimilarity/target
jdk=/home/khui/workspace/javaworkspace/java-8-sun/jdk1.8.0_45
log4jconf=/home/khui/workspace/javaworkspace/log4j.xml
tmpdir=/GW/D5data-2/khui/cw-docvector-termdf/tmp

source /home/khui/workspace/pyenv/startpy27.sh
pythondir="/home/khui/workspace/IdeaProjects/docsimilarity/src/main/resources/python/ecir17"

START=$(date +%s.%N)
islocal=true
if ! $islocal
then
	echo run on the cluster
	# for cluster
	spark-submit \
	--master yarn://139.19.52.105 \
	--driver-memory 20G \
	--executor-memory 40G \
	--executor-cores 8 \
	--num-executors 4 \
	--driver-java-options "-Djava.io.tmpdir=$tmpdir" \
	$pythondir/data4crowdflower.py
else
	echo run locally
	spark-submit \
	--master local[*] \
	--driver-memory 55G \
	--driver-java-options "-Djava.io.tmpdir=$tmpdir" \
	$pythondir/data4crowdflower.py
fi
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)
echo $expid finished within $DIFF


