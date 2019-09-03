# run polyphen suite for a sample and chromosome

. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
POLYDIR=$APPHOME/polyphen
LOGDIR=$APPHOME/logs
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
# set -Djava.security.egd to avoid JDBC connection reset when using many processes: https://community.oracle.com/thread/943911
DBCONN_INFO="-Dspring.config=../properties/marcus.xml -Djava.security.egd=file:/dev/../dev/urandom"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=hsnalabolu@mcw.edu,mtutaj@mcw.edu



echo "  running polyphen, custom fasta, for sample $1 and chr$2"
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar \
  --tool Polyphen --fasta \
  --sample $1 --chr $2 \
  --outDir $POLYDIR \
  --resultsDir $POLYDIR \
  > $LOGDIR/polyfa$1.$2.log


echo "  starting polyphen ..."


if [ -s $POLYDIR/Sample$1.$2.PolyPhenInput.fasta ]
then
  ./run_pph.sh $1 $2 fasta
else
  echo "*** NO DATA -- ABORTING SAMPLE $1 chr $2"
  rm $POLYDIR/ErrorFile_Sample$1.$2.PolyPhen.error
  rm $POLYDIR/Sample$1.$2.PolyPhenInput
  rm $POLYDIR/Sample$1.$2.PolyPhenInput.info
  rm $POLYDIR/$1.$2.log
  rm $LOGDIR/polyfa$1.$2.log
  exit 5
fi



#load polyphen results into database
echo "  loading results $1.$2 into db..."
cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar \
  --tool PolyphenLoader \
  --sample $1 --chr $2 \
  --outputDir $POLYDIR \
  --resultsDir $POLYDIR \
  > $LOGDIR/polyload$1.$2.log
echo `cat $LOGDIR/polyload$1.$2.log`

mailx -s "[$SERVER] polyphen load OK for $1.$2" $EMAIL_LIST < $LOGDIR/polyload$1.$2.log

echo "*** ALL DONE!***"

