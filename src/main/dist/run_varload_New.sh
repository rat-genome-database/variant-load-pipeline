# load variants from database to new structure in common format
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
DBCONN_INFO=-Dspring.config=../properties/variant.xml
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=hsnalabolu@mcw.edu


cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar \
  --tool VariantRatLoaderFromDb \
  --sampleId $1 \
  --chr $2 \
  > varload.log

mailx -s "[$SERVER] ratStrainLoader variantLoad OK" $EMAIL_LIST < varload.log

