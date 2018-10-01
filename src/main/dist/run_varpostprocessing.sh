# run variant post processing for one or more samples
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
DBCONN_INFO=-Dspring.config=../properties/reed.xml
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool VariantPostProcessing \
  --fastaDir /data/ref/fasta/rn5 \
  --verifyIfInRgd \
  --sampleId 711 \
  --sampleId 712 \
  --sampleId 713 \
  --sampleId 714 \
  --sampleId 715 \
  --sampleId 716 \
  --sampleId 717 \
  --sampleId 718 \
  --sampleId 719 \
  > varpostproc.log

mailx -s "[$SERVER] ratStrainLoader variantPostProcessing OK" $EMAIL_LIST < varpostproc.log
