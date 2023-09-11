
for fil in `ls *.txt`
do
  echo "ON FILE "$fil
  python3 munge2.py $fil
  rm logs*gz
  rm ready*json
done
