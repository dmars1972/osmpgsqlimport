#psql -d nav -h rpi16 -f create.sql
#psql -d nav -h rpi16 -f create_airports.sql
./build/osm_import -i planet-260601.osm.pbf -d nav -s rpi16 -u daniel -t 3 -w 6 -n 35000000000 -v -l osm.log -f /nodes/nodes.dat -S /data/nodes -R ways
