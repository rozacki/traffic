# concatenates all files in data/sites folder into single json
cd data/sites
find . *.json | awk {'print $1, " "'} > list_files
cat list_files | xargs cat > concateneted.json