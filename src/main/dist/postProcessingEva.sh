. /etc/profile
APPHOME=/home/rgddata/pipelines/ratStrainLoader
LOG4J_INFO="-Dlog4j.configurationFile=file://$APPHOME/properties/log4j.properties"
DBCONN_INFO="-Dspring.config=$APPHOME/../properties/default_db2.xml -Djava.security.egd=file:/dev/../dev/urandom"
JAVA_INFO="$DBCONN_INFO $LOG4J_INFO -jar lib/ratStrainLoader.jar"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu,llamers@mcw.edu
WORKDIR=$APPHOME/eva

cd $APPHOME

for mKey in "$@"
do
    if  [[ mKey -eq 380 ]]; then
        java $JAVA_INFO \
          --tool VariantPostProcessing \
          --mapKey 380 --fastaDir "/ref/fasta/GRCr8/" \
#          --verifyIfInRgd \
          > "$WORKDIR/vpp380.log"
        wait
        echo "  EVA species Rat 380 OK"
    fi

  if  [[ mKey -eq 372 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 372 --fastaDir "/ref/fasta/rn7.2/" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp372.log"
      wait
      echo "  EVA species Rat 372 OK"
  fi

  if  [[ mKey -eq 360 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 360 --fastaDir "/ref/fasta/rn6" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp360.log"
      wait
      echo "  EVA species Rat 360 OK"
  fi

  if  [[ mKey -eq 70 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 70 --fastaDir "/ref/fasta/rn5" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp70.log"
      wait
      echo "  EVA species Rat 70 OK"
  fi

  if  [[ mKey -eq 631 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 631 --fastaDir "/ref/fasta/canFam3" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp631.log"
      wait
      echo "  EVA species Dog 361 OK"
  fi

    if  [[ mKey -eq 634 ]]; then
        java $JAVA_INFO \
          --tool VariantPostProcessing \
          --mapKey 634 --fastaDir "/ref/fasta/ROS_Cfam_1.0" \
          --verifyIfInRgd \
          > "$WORKDIR/vpp634.log"
        wait
        echo "  EVA species Dog 364 OK"
    fi

  if  [[ mKey -eq 1311 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 1311 --fastaDir "/ref/fasta/chlSab2" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp1311.log"
      wait
      echo "  EVA species Green Monkey OK"
  fi

  if  [[ mKey -eq 910 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 910 --fastaDir "/ref/fasta/susScr3" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp910.log"
      wait
      echo "  EVA species Pig 10.2 OK"
  fi

  if  [[ mKey -eq 911 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 911 --fastaDir "/ref/fasta/susScr11" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp911.log"
      wait
      echo "  EVA species Pig 11.1 OK"
  fi

  if  [[ mKey -eq 35 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 35 --fastaDir "/ref/fasta/mm38" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp35.log"
      wait
      echo "  EVA species Mouse 38 OK"
  fi

  if  [[ mKey -eq 239 ]]; then
      java $JAVA_INFO \
        --tool VariantPostProcessing \
        --mapKey 239 --fastaDir "/ref/fasta/mm39" \
        --verifyIfInRgd \
        > "$WORKDIR/vpp239.log"
      wait
      echo "  EVA species Mouse 39 OK"
   fi
done