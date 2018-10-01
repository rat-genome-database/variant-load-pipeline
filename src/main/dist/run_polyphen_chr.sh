# run polyphen suite for a sample and chromosome

. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOGDIR=$APPHOME/logs
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
# set -Djava.security.egd to avoid JDBC connection reset when using many processes: https://community.oracle.com/thread/943911
DBCONN_INFO="-Dspring.config=../properties/reed.xml -Djava.security.egd=file:/dev/../dev/urandom"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
$OUTDIR=/data/rat/output
$RESDIR=/data/rat/results


# generate polyphen input file
echo ""
echo "generating polyphen input file for sample $1 and chr$2"
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool Polyphen \
  --sample $1 --chr $2 \
  --outDir $OUTDIR \
  2>&1 > $LOGDIR/poly$1.$2.log


# run polyphen
echo "  running polyphen for sample $1 and chr$2"
./run_pph.sh $1 $2

#load polyphen results into database
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool PolyphenLoader \
  --sample $1 --chr $2 \
  --outputDir $OUTDIR \
  --resultsDir $RESDIR \
  > $LOGDIR/polyload$1.$2.log
echo `cat $LOGDIR/polyload$1.$2.log`

# rerun polyphen for proteins that are not in polyphen database
# by supplying a fasta file with those proteins to the polyphen

echo "  re-running polyphen, custom fasta, for sample $1 and chr$2"
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool PolyphenFasta \
  --sample $1 --chr $2 \
  --outputDir $OUTDIR \
  --resultsDir $RESDIR \
  > $LOGDIR/polyfa$1.$2.log

if [ -s $OUTDIR/Sample$1.$2.PolyPhenInput.fasta ]
then
  ./run_pph.sh $1 $2 fasta
fi


#load polyphen results into database
echo "  loading results $1.$2 into db..."
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool PolyphenLoader \
  --sample $1 --chr $2 \
  --outputDir $OUTDIR \
  --resultsDir $RESDIR \
  > $LOGDIR/polyload$1.$2.log
echo `cat $LOGDIR/polyload$1.$2.log`

mailx -s "[$SERVER] polyphen load OK for $1.$2" $EMAIL_LIST < $LOGDIR/polyload$1.$2.log

echo "*** ALL DONE!***"

