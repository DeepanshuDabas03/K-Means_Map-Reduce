import sys
import os
import grpc
import random
from concurrent import futures
import kmeans_pb2 #type: ignore
import kmeans_pb2_grpc #type: ignore


def load_key_value_pairs(mapper_addresses, reducer_id):
    all_data = []
    for address in mapper_addresses:
        with grpc.insecure_channel(address) as channel:
            stub = kmeans_pb2_grpc.MapperServiceStub(channel)
            response = stub.GetPartitionData(kmeans_pb2.PartitionDataRequest(reducer_id=reducer_id))
            all_data.extend(response.key_value_pairs) 
    return all_data

def calculate_new_centroid(points,centroid_id):
    dimension = len(points[0].coordinates)
    sum_vector = [0.0 for _ in range(dimension)]

    for point in points:
        for i in range(dimension):
            sum_vector[i] += point.coordinates[i]

    return kmeans_pb2.Centroid(
        id=centroid_id,
        coordinates=[coord / len(points) for coord in sum_vector]
    )


class ReducerServer(kmeans_pb2_grpc.ReducerServiceServicer,kmeans_pb2_grpc.MapperService):
    def __init__(self,reducer_id):
        self.reducerId=reducer_id
        self.log(f"Reducer {self.reducerId}: Initialized on port {6000 + reducer_id}")
    def log(self,message):
        os.makedirs("Data/dump", exist_ok=True)
        with open(f'Data/dump/Reducer{self.reducerId}.txt', "a") as f:
            f.write(message + "\n")
    def RunReduceTask(self, request, context):
        centroids = request.centroids
        iteration_number=request.iteration_number
        mapper_addresses=request.mapper_addresses
        self.log(f"Iteration Number: {iteration_number}")
        self.log(f"Reducer {self.reducerId}: Starting reduce task, mapper addresses: {mapper_addresses}")

        # Simulate fault tolerance
        if random.random() < 0.2:  # Probability for failure is 0.5
            self.log("Reducer task failed due to simulated fault")
            return kmeans_pb2.ReducerResponse(status=kmeans_pb2.ReducerResponse.Status.FAILURE)

        key_value_pairs = load_key_value_pairs(mapper_addresses, self.reducerId)

        self.log(f"Reducer {self.reducerId}: Fetched data from mappers") 
        self.log(f"Fetched data from mappers {key_value_pairs}")

        # Group points by centroid ID, Shuffle and Sort
        points_by_centroid = {}
        for x in key_value_pairs:
            points_by_centroid.setdefault(x.key, []).append(x.value)

        # Calculate new centroids
        new_centroids = []
        for centroid_id, points in points_by_centroid.items():
            new_centroid = calculate_new_centroid(points=points,centroid_id=centroid_id)
            new_centroids.append(new_centroid)
        # Write new centroids to a file
        os.makedirs(f"Data/Reducers", exist_ok=True)
        with open(f"Data/Reducers/R{self.reducerId}.txt", "w") as f:
            for centroid in new_centroids:
                f.write(f"{centroid.id},{','.join(str(x) for x in centroid.coordinates)}")
            self.log(f"Reducer {self.reducerId}: Completed reduce task")
        return kmeans_pb2.ReducerResponse(status=kmeans_pb2.ReducerResponse.Status.SUCCESS)

    def GetCentroids(self, request, context):
        os.makedirs(f"Data/Reducers", exist_ok=True)
        centroid=kmeans_pb2.GetCentroidsResponse()
        with open(f"Data/Reducers/R{self.reducerId}.txt", "r") as f:
            for l in f:
                l=l.strip('\n')
                d=[]
                for i in l.split(","):
                    d.append(i)
                centroid.centroids.add(id=int(d[0]),coordinates=[float(x) for x in d[1:]])
        return centroid
      

if __name__ == "__main__":
    reducer_id = int(sys.argv[1])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  
    kmeans_pb2_grpc.add_ReducerServiceServicer_to_server(ReducerServer(reducer_id=reducer_id), server)
    try:
        server.add_insecure_port(f'[::]:{6000 + reducer_id}') 
        server.start()
        server.wait_for_termination()
    except Exception as e:
        print("Error starting server for reducer")
        server.stop(0)
        exit(0)
    


