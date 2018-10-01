# load conservation score table
#
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO="-Dlog4j.configuration=file://$APPHOME/properties/log4j.properties"
DBCONN_INFO="-Dspring.config=$APPHOME/../properties/default_db.xml"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
WORKDIR=$APPHOME

# abort the script if any of stages below will fail
set -e

cd $APPHOME

java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool ConservationScore \
    --fileName /data/ref/phastCons100/hg38.phastCons100way.wig.gz \
    --tableName CONSERVATION_SCORE_IOT_HG38 \
    --dataSourceName ConScore \
    | tee "$WORKDIR/conscore_hg38.log"
