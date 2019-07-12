"""
this will extract the raw text files from the MTA into something
more structured and tabular.

from those tables we will be able to build our transit graph
"""


"""
files:
    raw_data/mta/google_transit/calendar_dates.txt
    raw_data/mta/google_transit/stop_times.txt
    raw_data/mta/google_transit/transfers.txt
    raw_data/mta/google_transit/agency.txt
    raw_data/mta/google_transit/stops.txt
    raw_data/mta/google_transit/trips.txt
    raw_data/mta/google_transit/shapes.txt
    raw_data/mta/google_transit/calendar.txt
    raw_data/mta/google_transit/routes.txt
"""

import csv
import collections
import redis
import time
import os
print("in a file right here")

def open_trans_file(file_name):
    with open(file_name) as f:
        return list(csv.DictReader(f))

class GraphClient:
    def __init__(self, graph_name):
        self.graph_name = graph_name
        self.connection = None

    def conn(self):
        if self.connection is None:
            self.connection = redis.StrictRedis()
        return self.connection

    def execute(self, query):
        reply = self.conn().execute_command('GRAPH.QUERY', self.graph_name, query)
        return reply

    def delete(self):
        return self.conn().execute_command("GRAPH.DELETE", self.graph_name)

    def execution_plan(self, query):
        """
        Get the execution plan for given query,
        GRAPH.EXPLAIN returns an array of operations.
        """
        plan = self.conn().execute_command("GRAPH.EXPLAIN", self.graph_name, query)
        return plan


def escape(s, character):
    return s.replace(character, '\\' + character)


def load_stations(client, stations):
    cleaned = [
        {
            'stop_id': escape(s['stop_id'], "'"),
            'stop_code': escape(s['stop_code'], "'"),
            'stop_name': escape(s['stop_name'], "'"),
            'stop_desc': escape(s['stop_desc'], "'"),
            'stop_lat': float(s['stop_lat']),
            'stop_lon': float(s['stop_lon']),
            'zone_id': escape(s['zone_id'], "'"),
            'stop_url': escape(s['stop_url'], "'"),
            'location_type': escape(s['location_type'], "'"),
            'parent_station': escape(s['parent_station'], "'")
        } for s in stations
    ]
    for row in cleaned:
        client.execute("""
            CREATE
            (n:Stop {{
                stop_id: '{stop_id}',
                stop_code: '{stop_code}',
                stop_name: '{stop_name}',
                stop_desc: '{stop_desc}',
                stop_lat: '{stop_lat}',
                stop_lon: '{stop_lon}',
                zone_id: '{zone_id}',
                stop_url: '{stop_url}',
                location_type: '{location_type}',
                parent_station: '{parent_station}'
            }})
        """.format(
            stop_id=row['stop_id'],
            stop_code=row['stop_code'],
            stop_name=row['stop_name'],
            stop_desc=row['stop_desc'],
            stop_lat=row['stop_lat'],
            stop_lon=row['stop_lon'],
            zone_id=row['zone_id'],
            stop_url=row['stop_url'],
            location_type=row['location_type'],
            parent_station=row['parent_station'],
        ))


def load_stop_times(client, stop_times, stops):
    """
    OrderedDict([('trip_id', 'ASP19GEN-1037-Sunday-00_000600_1..S03R'),
                 ('arrival_time', '00:06:00'),
                 ('departure_time', '00:06:00'),
                 ('stop_id', '101S'),
                 ('stop_sequence', '1'),
                 ('stop_headsign', ''),
                 ('pickup_type', '0'),
                 ('drop_off_type', '0'),
                 ('shape_dist_traveled', '')])
    """

    for s in stop_times:
        s['raw_stop_id'] = s['stop_id']
        matches = [stop['stop_id'] for stop in stops if s['stop_id'] == stop['stop_id']]
        if len(matches) == 0:
            raise Exception('not found {}'.format(s['raw_stop_id']))
        s['stop_id'] = matches[0]

    cleaned = [
        {
            'trip_id': s['trip_id'],
            'arrival_time': s['arrival_time'],
            #'departure_time': s['departure_time'],
            'stop_id': s['stop_id'],
            'raw_stop_id': s['raw_stop_id'],
            'stop_sequence': int(s['stop_sequence']),
            'stop_headsign': s['stop_headsign'],
            'pickup_type': s['pickup_type'],
            'drop_off_type': s['drop_off_type']
        } for s in stop_times
    ]
    for row in cleaned:
        client.execute("""
        CREATE
        (n:StopTime {{
            trip_id: '{trip_id}',
            arrival_time: '{arrival_time}',
            stop_id: '{stop_id}',
            raw_stop_id: '{raw_stop_id}',
            stop_sequence: {stop_sequence},
            stop_headsign: '{stop_headsign}',
            pickup_type: '{pickup_type}',
            drop_off_type: '{drop_off_type}'
        }})
        """.format(
            trip_id=row['trip_id'],
            arrival_time=row['arrival_time'],
            stop_id=row['stop_id'],
            raw_stop_id=row['raw_stop_id'],
            stop_sequence=row['stop_sequence'],
            stop_headsign=row['stop_headsign'],
            pickup_type=row['pickup_type'],
            drop_off_type=row['drop_off_type']
        ))


def load_trips(client, trips):
    """
    """
    cleaned = [
        {
            'route_id': s['route_id'],
            'service_id': s['service_id'],
            'trip_id': s['trip_id'],
            'trip_headsign': escape(s['trip_headsign'], "'"),
            'direction_id': s['direction_id'],
            'block_id': s['block_id']
            # shape_id
        } for s in trips
    ]
    for row in cleaned:
        client.execute("""
            CREATE
            (n:Trip {{
                route_id: '{route_id}',
                service_id: '{service_id}',
                trip_id: '{trip_id}',
                trip_headsign: '{trip_headsign}',
                direction_id: '{direction_id}',
                block_id: '{block_id}'
            }})
        """.format(
            route_id=row['route_id'],
            service_id=row['service_id'],
            trip_id=row['trip_id'],
            trip_headsign=row['trip_headsign'],
            direction_id=row['direction_id'],
            block_id=row['block_id']
        ))

def load_data(c, trips, stops, stop_times):
    """
    for the given datasets, load a subset into redisgraph
    """

    # filter down to a reasonable subset
    trips = [t for t in trips if t['route_id'].startswith("1") or
             t['route_id'].startswith("2")]
    trips = trips[:800]
    allowed_trains = set(i['trip_id'] for i in trips)
    stop_times = [s for s in stop_times if s['trip_id'] in allowed_trains]
    allowed_stops = list(set(i['stop_id'] for i in stop_times))
    stops = [s for s in stops if any(True for i in allowed_stops if
                                     i.startswith(s['stop_id']))]

    load_stations(c, stops)
    load_trips(c, trips)
    load_stop_times(c, stop_times, stops)

def main(data_dir, output_dir):
    c = GraphClient('subway')
    trips = open_trans_file(
        os.path.join(data_dir, 'mta/google_transit/trips.txt')
    )
    stops = open_trans_file(
        os.path.join(data_dir, 'mta/google_transit/stops.txt')
    )
    stop_times = open_trans_file(
        os.path.join(data_dir, 'mta/google_transit/stop_times.txt')
    )

    start = time.time()
    load_data(c, trips, stops, stop_times)
    stop = time.time()
    print("time loading data {} ms".format(1000 * (stop - start)))

    print("number of nodes to search for edges:")
    print(c.execute("match (n:StopTime) return count(n)"))
    print('running edge-finding test')
    # performance
    start = time.time()
    print(
        c.execute("""
        MATCH (_from:StopTime), (_to:StopTime)
        WHERE _from.trip_id = _to.trip_id and (_from.stop_sequence + 1) = _to.stop_sequence
        CREATE (_from)-[r:NEXT_STOP]->(_to)
        """)
    )
    stop = time.time()
    print("time finding-edges: {}ms".format(1000 * (stop - start)))
    c.execute("match (n) delete n")
    c.delete()

if __name__ == '__main__':
    data_dir = os.environ.get('DATA_DIR')
    output_dir = os.environ.get('OUTPUT_DIR')
    main(data_dir, output_dir)



# c.execute("match (n) return count(n)")
# c.execute("match (n:Trip) return count(n)")
# c.execute("match (n:Stop) return count(n)")
# c.execute("match (n:StopTime) return count(n)")
# 
# c.execute("match (n) delete n")
# c.delete()
# 
# 
# 
# 
# # crashes redisgraph
# c.execute("""
# MATCH (n:Trip) where n.trip_id = 'ASP19GEN-1087-Weekday-00_149900_1..N03R'
# create (n:TempTrip)
# """)
# 
# c.execute("""
# MATCH (n:Trip) where n.trip_id = 'ASP19GEN-2042-Saturday-00_001900_2..S08R'
# RETURN n
# limit 1
# """)
# 
# c.execute("""
# MATCH (n:StopTime) where n.trip_id = 'ASP19GEN-2042-Saturday-00_001900_2..S08R'
# RETURN n
# """)
# 
# 
# c.execute("""
# MATCH (train:Trip)
# MATCH (stop:StopTime) WHERE stop.trip_id = train.trip_id
# CREATE (train)-[r:ARRIVAL]->(stop)
# RETURN (stop)
# """)
# 
# c.execute("""
# MATCH (stop:Stop)
# MATCH (stoptime:StopTime) WHERE stoptime.stop_id = stop.stop_id
# CREATE (stoptime)-[r:STATION]->(stop)
# """)
# 
# start = time.time()
# c.execute("""
# MATCH (_from:StopTime), (_to:StopTime)
# WHERE _from.trip_id = _to.trip_id and (_from.stop_sequence + 1) = _to.stop_sequence
# CREATE (_from)-[r:NEXT_STOP]->(_to)
# """)
# stop = time.time()
# print(stop - start)
# 
# c.execute("""
# match (n:Stop)
# return n
# """)
# 
# c.execute("""
# match (n:Stop)<-[:STATION]-(e)
# where n.stop_name = 'Bergen St'
# return e
# """)
# 
# c.execute("""
# match (n:Stop)
# where n.stop_name = 'Chambers St'
# return n
# """)
# 
# c.execute("""
# match (start:Stop {stop_name: 'Bergen St' })
# <-[:STATION]-
# (s:StopTime {arrival_time: '06:17:30'})
# -[x]->(end:StopTime)-[:STATION]->(:Stop {stop_name: 'Chambers St'})
# return x.arrival_time
# """)
# 
# 
# c.execute("""
# match
# (s:StopTime {trip_id: 'ASP19GEN-2048-Sunday-00_036150_2..N08R'})
# -[]->(n:StopTime)
# return s
# """)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# len(
# collections.Counter(
#     r['trip_id'] for r in stop_times
# )
# )
# len(stop_times)
# 
# stop_times[0]
# 
# stops = open_trans_file('raw_data/mta/google_transit/stops.txt')
# len(stops)
# 
# stops[len(stops)//2]
# 
# 
# routes = open_trans_file('raw_data/mta/google_transit/routes.txt')
# 
# routes[0]
# 
# len(routes)
# 
# collections.Counter(
#     r['route_id'] for r in routes
# )
# 
# def count(table, field):
#     return collections.Counter(
#         r[field] for r in table
#     )
# 
# 
# trips = open_trans_file('raw_data/mta/google_transit/trips.txt')
# len(trips)
# 
# len(count(trips, 'route_id'))
# 
# [t for t in routes if t['route_id'] == 'GS']
# [t for t in trips if t['route_id'] == 'GS']
# 
# trips[0]
# 
# 
# transfers = open_trans_file('raw_data/mta/google_transit/transfers.txt')
# len(transfers)
# transfers[0]
# count(transfers, 'transfer_type')
# 
# len([i for i in transfers if i['from_stop_id'] == i['to_stop_id']])
# # SO MANY inner station transfers what the fuck
# 
# # what are the outer station transfers?
# [i for i in transfers if i['from_stop_id'] != i['to_stop_id']]
# 
# # you can transfer from stop 101 to stop 101? does this imply there are more
# # than one train that hits stop 101?
# # how do i find that out?
# 
# [i for i in stops if i['stop_id'] == '101']
# trips[0]
# stops[0]
# 
# trip_ids = set(i['trip_id'] for i in stop_times if
#                i['stop_id'].startswith('101'))
# matching_trips = [i for i in trips if i['trip_id'] in trip_ids]
# count(matching_trips, 'route_id')
# 
# 
# # inner tranfsers don't make sense yet.
# # what are the stations WITHOUT an inner transfer?
# inner_transfers = [i['from_stop_id'] for i in transfers if i['from_stop_id'] == i['to_stop_id']]
# 
# len(inner_transfers)
# inner_transfers[30]
# for station in inner_transfers[:30]:
#     trip_ids = set(i['trip_id'] for i in stop_times if
#                    i['stop_id'].startswith(station))
# 
#     this_guy = set(i['route_id'] for i in trips if i['trip_id'] in trip_ids)
#     print(station, this_guy)
# 
# station_to_matching_trip_ids = {
#     station: set(
#         i['trip_id'] for i in stop_times if i['stop_id'].startswith(station)
#     ) for station in inner_transfers
# }
# for station in station_to_matching_trip_ids:
#     trip_ids = station_to_matching_trip_ids[station]
#     this_guy = set(i['route_id'] for i in trips if i['trip_id'] in trip_ids)
#     stop_name = next(i['stop_name'] for i in stops if i['stop_id'] == station)
#     print(stop_name, this_guy)
# 
# # okay, inner station transfers make sense. multiple lines hitting one station
# # if i were to model this i would not make this distinction.
# 
# 
# 
# # what about a couple outer transfers?
# """
#  OrderedDict([('from_stop_id', '132'),
#               ('to_stop_id', 'D19'),
#               ('transfer_type', '2'),
#               ('min_transfer_time', '300')]),
#  OrderedDict([('from_stop_id', '132'),
#               ('to_stop_id', 'L02'),
#               ('transfer_type', '2'),
#               ('min_transfer_time', '180')]),
# """
# 
# outer_transfers = [(i['from_stop_id'], i['to_stop_id']) for i in transfers if i['from_stop_id'] != i['to_stop_id']]
# for _from, _to in outer_transfers:
#     from_name = next((_from, i['stop_name']) for i in stops if i['stop_id'] == _from)
#     to_name = next((_to, i['stop_name']) for i in stops if i['stop_id'] == _to)
#     print(from_name, '\t->', to_name)
# 
# 
# """
# plan:
#     routes (29)
#         named subway routes, like the A train, C train, 1 train
#     trips (19842)
#         a start-to-end subway route instance. like a train id. one train passes
#         through many stops, each at a designated stop time
#     stops (1503)
#         stations
#     stop_times (550495)
#         time at which a given train will stop at a given station
# """
# 
# c = GraphClient('social')
# 
# c.execution_plan("""
#     CREATE
#     (:person {name: 'roi', age: 33, gender: 'male', status: 'married'})
# """)
# 
# stops = open_trans_file('raw_data/mta/google_transit/stops.txt')
# 
# len(stops)
# stops[0]
# 
