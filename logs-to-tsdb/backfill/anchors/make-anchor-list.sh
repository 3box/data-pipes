
for fil in `ls *.txt`
do
  python3 munge-anchors.py $fil
done
