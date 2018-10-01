# load variants from sample's files in common format
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
DBCONN_INFO=-Dspring.config=../properties/reed.xml
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

INDIR=/data/RatCarpeNovoData/processedInput/rn5_mcw9genomes

cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
  --tool VariantLoad3 \
  --sampleId 711 --inputFile $INDIR/ACI \
  --sampleId 712 --inputFile $INDIR/COP \
  --sampleId 713 --inputFile $INDIR/FHH \
  --sampleId 714 --inputFile $INDIR/FHL \
  --sampleId 715 --inputFile $INDIR/GH \
  --sampleId 716 --inputFile $INDIR/SBH \
  --sampleId 717 --inputFile $INDIR/SBN \
  --sampleId 718 --inputFile $INDIR/SR \
  --sampleId 719 --inputFile $INDIR/SS \
  > varload.log

mailx -s "[$SERVER] ratStrainLoader variantLoad OK" $EMAIL_LIST < varload.log
