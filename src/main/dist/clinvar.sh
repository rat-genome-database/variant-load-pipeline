# ClinVar data for VariantVisualizer tool
#  2 sets of data will be generated and updated weekly
#  sample_id=1: ClinVar data for assembly hg37 (mapKey=17)
#  sample_id=2: ClinVar data for assembly hg38 (mapKey=38)
#
. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO="-Dlog4j.configuration=file://$APPHOME/properties/log4j.properties"
DBCONN_INFO="-Dspring.config=$APPHOME/../properties/default_db.xml -Djava.security.egd=file:/dev/../dev/urandom"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
WORKDIR=$APPHOME/clinvar

# abort the script if any of stages below will fail
set -e

STAGE1=1 #generate vcf
STAGE2=1 #vcf to txt
STAGE3=1 #load txt
STAGE4=1 #vpp

cd $APPHOME
if [[ $STAGE1 -eq 1 ]]; then
echo "STAGE1: generate vcf files from ClinVar variants"

  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool ClinVar2Vcf \
    --mapKey 17 --outputFile "$WORKDIR/ClinVar37.vcf.gz" \
    > "$WORKDIR/clinvar2vcf37.log" &

  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool ClinVar2Vcf \
    --mapKey 38 --outputFile "$WORKDIR/ClinVar38.vcf.gz" \
    > "$WORKDIR/clinvar2vcf38.log" &

  echo "  vcf generation launched in 2 processes..."
  wait
  echo "  vcf generation OK"
else
  echo "STAGE1: skipped"
fi

if [[ $STAGE2 -eq 1 ]]; then
  echo "STAGE2: convert vcf files to common format"
  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VcfConverter2 \
    --mapKey 17 \
    --vcfFile "$WORKDIR/ClinVar37.vcf.gz" \
    --outDir "$WORKDIR" \
    --compressOutputFile \
    > "$WORKDIR/vcf2txt37.log" &

  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VcfConverter2 \
    --mapKey 38 \
    --vcfFile "$WORKDIR/ClinVar38.vcf.gz" \
    --outDir "$WORKDIR" \
    --compressOutputFile \
    > "$WORKDIR/vcf2txt38.log" &
  wait
  echo "  STAGE2: OK"
else
  echo "STAGE2: skipped"
fi

if [[ $STAGE3 -eq 1 ]]; then
  echo "STAGE3: load ClinVar common format files into RATCN database"
  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VariantLoad3 \
    --sampleId 1 --inputFile "$WORKDIR/ClinVar17.txt.gz" \
    --verifyIfInRgd \
    > "$WORKDIR/load37.log" &

  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VariantLoad3 \
    --sampleId 2 --inputFile "$WORKDIR/ClinVar38.txt.gz" \
    --verifyIfInRgd \
    > "$WORKDIR/load38.log" &

  wait
  echo "  STAGE3: OK"
else
  echo "STAGE3: skipped"
fi

if [[ $STAGE4 -eq 1 ]]; then
  echo "STAGE4: run variant post processing on loaded samples"
  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VariantPostProcessing \
    --sampleId 1 --fastaDir "/data/ref/fasta/hs37" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp37.log" &

  java $DBCONN_INFO $LOG4J_INFO -jar ratStrainLoader.jar \
    --tool VariantPostProcessing \
    --sampleId 2 --fastaDir "/data/ref/fasta/hs38" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp38.log" &
  wait
  echo "  STAGE4: ok"
else
  echo "STAGE4: skipped"
fi


echo "STAGE5: TODO: run polyphen"
