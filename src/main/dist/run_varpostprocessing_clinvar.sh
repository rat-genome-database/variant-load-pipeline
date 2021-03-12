# run variant post processing for one or more samples
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
DBCONN_INFO=-Dspring.config=../properties/variant.xml
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=hsnalabolu@mcw.edu

cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar \
  --tool VariantPostProcessing \
  --fastaDir /data/ref/fasta/hs37 \
  --mapKey 17 \
  --chr $1 \
  > varpostproc1.log

mailx -s "[$SERVER] ratStrainLoader variantPostProcessing OK" $EMAIL_LIST < varpostproc.log



