# run variant transcripts for a range of samples and a given chromosome
# f.e.
#   run_varpostprocessing_loop.sh
#
for ((s=1; s<=20; s+=1));
do
  echo "RUN VAR TRANS FOR  chr $s"
  ./run_varpostprocessing_New.sh $s
done

echo "--- DONE FOR MAPKEY 70  ---"
