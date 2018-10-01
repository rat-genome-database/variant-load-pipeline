# convert vcf files into common format
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO=-Dlog4j.configuration=file:$APPHOME/properties/log4j.properties
DBCONN_INFO=-Dspring.config=../properties/reed.xml
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

VCF_FILE=/data/rnor5_9genomes/9Genomes_snps_mbq20_raw.vcf
OUT_DIR=/data/RatCarpeNovoData/processedInput/rn5_mcw9genomes

cd $APPHOME
java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar --tool VcfConverter2 --vcfFile $VCF_FILE --outDir $OUT_DIR > converter.log
mailx -s "[$SERVER] ratStrainLoader vcfConverter OK" $EMAIL_LIST < converter.log
