#!/bin/bash
# usage    : run_pph.sh sampleId chr
#       i.e. nohup ./run_pph.sh 208 1
#
# wrapper script, based on README file from polyphen-2.2.2, to run polyphen
#

export PPH="/data/polyphen-2.2.2"
ODIR=/data/rat/output
RDIR=/data/rat/results

if [ "$2" = "" ]
then
  export chr=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 X MT )
else
  export chr=( $2 )
fi

# if 3rd param is set to fasta, supply the fasta file as extra source of fasta sequences
if [ "$3" = "fasta" ]
then
  FASTA="-s $ODIR/Sample$1.$2.PolyPhenInput.fasta"
else
  FASTA=""
fi

for i in "${chr[@]}"
do
   rm  $RDIR/$1.$i.polyphen $RDIR/$1.$i.log 2> /dev/null

   M=13   # number of program instances to run
   for (( N=1; N<=$M; N++ )); do
     $PPH/bin/run_pph.pl -r $N/$M $FASTA $ODIR/Sample$1.$i.PolyPhenInput 1>$RDIR/$1.$i.weka$N 2> $RDIR/$1.$i.log$N &
   done
   wait
   rm -f $RDIR/$1.$i.weka $RDIR/$1.$i.log
   for (( N=1; N<=$M; N++ )); do
     cat $RDIR/$1.$i.weka$N >>$RDIR/$1.$i.weka
     cat $RDIR/$1.$i.log$N >>$RDIR/$1.$i.log
     rm -f $RDIR/$1.$i.weka$N $RDIR/$1.$i.log$N
   done

  $PPH/bin/run_weka.pl $RDIR/$1.$i.weka > $RDIR/$1.$i.polyphen
  rm -f $RDIR/$1.$i.weka
done
