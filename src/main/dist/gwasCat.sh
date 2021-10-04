. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO="-Dlog4j.configuration=file://$APPHOME/properties/log4j.properties"
DBCONN_INFO="-Dspring.config=$APPHOME/../properties/default_db.xml -Djava.security.egd=file:/dev/../dev/urandom"
JAVA_INFO="$DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu,llamers@mcw.edu
WORKDIR=$APPHOME/gwasCatalog

# abort the script if any of stages below will fail
set -e

STAGE1=1 #generate vcf
STAGE2=1 #vcf to txt
STAGE3=1 #load txt
STAGE4=1 #vpp

cd $APPHOME
if [[ $STAGE1 -eq 1 ]]; then
echo "STAGE1: generate vcf files from GWAS Catalog variants"

  java $JAVA_INFO \
    --tool GwasCat2Vcf \
    --mapKey 38 \
    --outputFile "c:/Github/variant-load-pipeline/src/main/dist/gwasCatalog/Gwas38.vcf.gz"\
    > "$WORKDIR/gwas2vcf38.log"

  echo "  vcf generation launched in 2 processes..."
  wait
  echo "  vcf generation OK"
else
  echo "STAGE1: skipped"
fi

if [[ $STAGE2 -eq 1 ]]; then
  echo "STAGE2: convert vcf files to common format"

  java $JAVA_INFO \
    --tool VcfConverter2 \
    --mapKey 38 \
    --vcfFile "$WORKDIR/Gwas38.vcf.gz" \
    --outDir "$WORKDIR" \
    --compressOutputFile \
    --ADDP \
    > "$WORKDIR/vcf2txt38.log"
  wait
  echo "  STAGE2: OK"
else
  echo "STAGE2: skipped"
fi

if [[ $STAGE3 -eq 1 ]]; then
  echo "STAGE3: load GWAS common format files into RATCN database"

  java $JAVA_INFO \
    --tool VariantLoad3 \
    --sampleId 3 --inputFile "$WORKDIR/Gwas38.txt.gz" \
#    --verifyIfInRgd \
    > "$WORKDIR/loadGWAS38.log"

  wait
  echo "  STAGE3: OK"
else
  echo "STAGE3: skipped"
fi

if [[ $STAGE4 -eq 1 ]]; then
  echo "STAGE4: run variant post processing on loaded samples"

  java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 38 --fastaDir "/data/ref/fasta/hs38" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp38.log"
  wait
  echo "  STAGE4: ok"
else
  echo "STAGE4: skipped"
fi
