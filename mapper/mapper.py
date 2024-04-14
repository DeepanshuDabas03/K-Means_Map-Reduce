import math
import os
import sys
import grpc
from concurrent import futures
import kmeans_pb2 #type:ignore
import kmeans_pb2_grpc  #type:ignore

def log(message, mapper_id):
    os.makedirs("dump", exist_ok=True)
    with open(f"dump/Mapper{mapper_id}.txt", "a") as f:
        f.write(message + "\n")

def load_points_from_range(data_range, mapper_id):
    start_line, end_line = (int(x) for x in data_range.split("-"))
    points = []
    with open("input/points.txt", "r") as f:
        for i, line in enumerate(f):
            if start_line <= i < end_line:
                coordinates = [float(x) for x in line.strip().split(", ")]
                points.append(kmeans_pb2.Point(coordinates=coordinates))
    log(f"Loaded {len(points)} points from range {data_range}", mapper_id)
    return points

def calculate_distance(point, centroid):
    x1, y1 = point.coordinates
    x2, y2 = centroid.coordinates
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return distance

def partition_output(key_value_pairs, num_reducers, mapper_id):
    partitions = [[] for _ in range(num_reducers)]
    for centroid_id, point in key_value_pairs:
        partition_id = centroid_id % num_reducers
        partitions[partition_id].append((centroid_id, point))
    log(f"Partitioned output into {num_reducers} partitions", mapper_id)
    return partitions

class MapperServer(kmeans_pb2_grpc.MapperServiceServicer):
    def __init__(self, num_reducers, mapper_id):
        self.reducers = num_reducers
        self.mapper_id = mapper_id

    def RunMapTask(self, request, context):
        points = load_points_from_range(request.data_split, self.mapper_id)
        centroids = request.centroids
        num_reducers = self.reducers
        iteration_number = request.iteration_number

        key_value_pairs = []
        for point in points:
            closest_centroid_id = None
            closest_distance = float('inf')

            for centroid in centroids:
                distance = calculate_distance(point, centroid)
                if distance < closest_distance:
                    closest_distance = distance
                    closest_centroid_id = centroid.id

            key_value_pairs.append((closest_centroid_id, point))
        
        # Partition Output
        partitions = partition_output(key_value_pairs, num_reducers, self.mapper_id)
        os.makedirs(f"M{mapper_id}", exist_ok=True) # Create directory if it doesn't exist
        for i, partition in enumerate(partitions):
            with open(f"M{self.mapper_id}/partition_{i}.txt", "w") as f: 
                for centroid_id, point in partition:
                    f.write(f"{centroid_id} {str(point)}\n")  # Adjust serialization if needed

        log(f"Completed map task for iteration {iteration_number}", self.mapper_id)
        return kmeans_pb2.MapperResponse(status=kmeans_pb2.MapperResponse.Status.SUCCESS)
    
    def GetPartitionData(self, request, context):
        reducer_id = request.reducer_id
        partition_file = f"M{self.mapper_id}/partition_{reducer_id}.txt"
        key_value_pairs = []
        if os.path.exists(partition_file):
            with open(partition_file, "r") as f:
                for line in f:
                    try:
                        centroid_id, point_str = line.strip().split()
                        point = kmeans_pb2.Point()  
                        # Deserialize the Point object (if needed)
                        key_value_pairs.append((int(centroid_id), point))
                    except Exception as e:
                        print(f"Error parsing line: {line} - {e}")

        return kmeans_pb2.PartitionData(key_value_pairs=key_value_pairs) 

if __name__ == "__main__":
    mapper_id = int(sys.argv[1])  #mapper ID as an argument 
    num_reducers = int(sys.argv[2])  #number of reducers as an argument
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  
    kmeans_pb2_grpc.add_MapperServiceServicer_to_server(MapperServer(num_reducers, mapper_id), server)
    log(f"Starting server on port {5000 + mapper_id}", mapper_id)
    server.add_insecure_port(f'[::]:{5000 + mapper_id}')
    try:  
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0) 
    except Exception as e:
        log(f"Unexpected error: {e}", mapper_id)
        server.stop(0)
