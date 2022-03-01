. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO="-Dlog4j.configuration=file://$APPHOME/properties/log4j.properties"
DBCONN_INFO="-Dspring.config=$APPHOME/../properties/default_db.xml -Djava.security.egd=file:/dev/../dev/urandom"
JAVA_INFO="$DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu,llamers@mcw.edu
WORKDIR=$APPHOME/eva

  java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 360 --fastaDir "/ref/fasta/rn6" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp360.log"
  wait
  echo "  EVA species Rat 360 OK"

    java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 70 --fastaDir "/ref/fasta/rn5" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp70.log"
  wait
  echo "  EVA species Rat 70 OK"

      java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 631 --fastaDir "/ref/fasta/canFam3" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp631.log"
  wait
  echo "  EVA species Dog OK"

        java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 1311 --fastaDir "/ref/fasta/chlSab2" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp1311.log"
  wait
  echo "  EVA species Green Monkey OK"

      java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 910 --fastaDir "/ref/fasta/susScr3" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp910.log"
  wait
  echo "  EVA species Pig 10.2 OK"

    java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 911 --fastaDir "/ref/fasta/susScr11" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp911.log"
  wait
  echo "  EVA species Pig 11.1 OK"

    java $JAVA_INFO \
    --tool VariantPostProcessing \
    --mapKey 35 --fastaDir "/ref/fasta/mm38" \
    --verifyIfInRgd \
    > "$WORKDIR/vpp35.log"
  wait
  echo "  EVA species Mouse OK"