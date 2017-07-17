DIR=$1

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit 1
fi

for file in $DIR/*.log; do
   grep 'done @' < "$file" | head -1  > "$file".txt
   grep 'done @' < "$file" | tail -1 >> "$file".txt 
 
   grep -v 'done @' < "$file" >> "$file".txt
   rm "$file"
done
