#!/bin/bash
# usage    : run_pph.sh sampleId chr
#       i.e. nohup ./run_pph.sh 208 1
#
# wrapper script, based on README file from polyphen-2.2.2, to run polyphen
#

export PPH="/data/polyphen-2.2.2"
POLYDIR=/data/pipelines/ratStrainLoader/polyphen

if [ "$2" = "" ]
then
  export chr=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 X MT )
else
  export chr=( $2 )
fi

# if 3rd param is set to fasta, supply the fasta file as extra source of fasta sequences
if [ "$3" = "fasta" ]
then
  FASTA="-s $POLYDIR/Sample$1.$2.PolyPhenInput.fasta"
else
  FASTA=""
fi

for i in "${chr[@]}"
do
   rm  $POLYDIR/$1.$i.polyphen $POLYDIR/$1.$i.log 2> /dev/null

   M=50   # number of program instances to run
   for (( N=1; N<=$M; N++ )); do
     $PPH/bin/run_pph.pl -r $N/$M $FASTA $POLYDIR/Sample$1.$i.PolyPhenInput 1>$POLYDIR/$1.$i.weka$N 2> $POLYDIR/$1.$i.log$N &
   done
   wait
   rm -f $POLYDIR/$1.$i.weka $POLYDIR/$1.$i.log
   for (( N=1; N<=$M; N++ )); do
     cat $POLYDIR/$1.$i.weka$N >>$POLYDIR/$1.$i.weka
     cat $POLYDIR/$1.$i.log$N >>$POLYDIR/$1.$i.log
     rm -f $POLYDIR/$1.$i.weka$N $POLYDIR/$1.$i.log$N
   done

  $PPH/bin/run_weka.pl $POLYDIR/$1.$i.weka > $POLYDIR/$1.$i.polyphen
  rm -f $POLYDIR/$1.$i.weka
done
