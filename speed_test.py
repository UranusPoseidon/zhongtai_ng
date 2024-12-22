import datetime
from pymongo import MongoClient
import psycopg2
from geopy.distance import geodesic
import re
import psycopg2

# Connect to MongoDB
mongo_client = MongoClient('mongodb://law.conetop.cn:27017/')
mongo_db = mongo_client['harbintrips']
mongo_collection = mongo_db['trips']
times = []
route_geom_tem=[]
route_geom=[] #each list include many tuples, every tuple include two parameters:(latitude,longitud)
roads=[]
longitudes=[]
latitudes=[]
id=''

def ComputationSpeed():
    #clean the route_geom
    for i in route_geom_tem:
        string_latitudes_longitudes=re.findall(r'\((.*?)\)',i[0])[0]
        temlist=string_latitudes_longitudes.split(',')
        finallist=[]
        for j in temlist:
            temfloat=j.split(' ')
            temfloat=list(filter(None, temfloat))
            fle=float(temfloat[0])
            fla=float(temfloat[1])
            temtuple = (fla, fle)
            finallist.append(temtuple)
        route_geom.append(finallist)
    formatted_times = [datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in times]
    time_intervals = []
    for i in range(1, len(formatted_times)):
        time1 = datetime.datetime.strptime(formatted_times[i-1], '%Y-%m-%d %H:%M:%S')
        time2 = datetime.datetime.strptime(formatted_times[i], '%Y-%m-%d %H:%M:%S')
        interval = (time2 - time1).total_seconds()
        time_intervals.append(interval)

    speeds = []

    # Calculate distances and speeds
    for i in range(1, len(latitudes)):
        # Get the previous and current latitude and longitude
        prev_coord = (latitudes[i-1], longitudes[i-1])
        curr_coord = (latitudes[i], longitudes[i])
        
        # Calculate the distance between the two points
        distance = geodesic(prev_coord, curr_coord).meters  # distance in meters
        
        # Get the time interval
        time_interval = time_intervals[i-1]  # time interval in seconds
        
        # Calculate speed (meters per second)
        if time_interval > 0:
            speed = distance / time_interval
        else:
            speed = 0  # Avoid division by zero
        speed*=3.6
        speeds.append(speed)
    return speeds

def SaveToSQL(oid,speeds,dbname,user,password,host,port):
    connection = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    
    write_list = []
    for i in range(len(speeds)):
        roa = roads[i] if i < len(roads) else None
        overspeed = speeds[i] > 50
        write_list.append((
            f"{oid}_{i}",  # id
            roa,           # speed (assumed this is the speed reference, double-check this)
            overspeed,     # is_overspeed
            longitudes[i], latitudes[i],   # start_coordinates
            longitudes[i+1], latitudes[i+1]  # end_coordinates
        ))

    cursor = connection.cursor()

    try:
        # Insert data
        insert_query = '''
        INSERT INTO road_speed (id, speed, is_overspeed, start_longitudes_latitudes, end_longitudes_latitudes) VALUES
        (%s, %s, %s, POINT(%s, %s), POINT(%s, %s))
        '''

        for record in write_list:
            cursor.execute(insert_query, record)

        connection.commit()

    except Exception as e:
        print("An error occurred:", e)

    finally:
        # Close cursor and connection
        cursor.close()
        connection.close()
        
for document in mongo_collection.find():
    if document:
        times = document.get('timestamp', [])
        route_geom_tem = document.get('route_geom', [])
        roads = document.get('roads', [])
        longitudes = document.get('longitudes', [])
        latitudes = document.get('latitudes', [])
        id=str(document.get('_id'))
    speeds_list=ComputationSpeed()
    print(id)
    SaveToSQL(id,speeds_list,'harbintrips','zhongtai','zt123!','law.conetop.cn','5432')
    times.clear()
    route_geom_tem.clear()
    roads.clear()
    longitudes.clear()
    latitudes.clear()
    id=''