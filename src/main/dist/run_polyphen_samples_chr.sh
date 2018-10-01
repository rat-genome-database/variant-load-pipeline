# run polyphen for a range of samples and a given chromosome
# f.e.
#   run_polyphen_samples_chr.sh 911 919 X
#
for ((s=$1; s<=$2; s+=1));
do
  echo "RUN POLY FOR SAMPLE $s and chr$3"
  ./run_polyphen_chr.sh $s $3
done

echo "--- DONE FOR SAMPLES $1-$2 and chr$3 ---"
