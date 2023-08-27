import json

india_states = json.load(open('states_india.geojson', 'r'))
for feature in india_states["features"]:
    print(feature["properties"]["st_nm"])



