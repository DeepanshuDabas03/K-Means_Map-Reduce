import sys
import os
import grpc
from concurrent import futures
import kmeans_pb2 #type: ignore
import kmeans_pb2_grpc #type: ignore


def load_key_value_pairs(mapper_addresses):
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


class ReducerServer(kmeans_pb2_grpc.ReducerServiceServicer,kmeans_pb2_grpc.Mapper):
    def __init__(self,reducer_id):
        self.reducer_id=reducer_id
    def log(self,message):
        os.makedirs("dump", exist_ok=True)
        with open(f"dump/Reducer{self.reducer_id}.txt", "a") as f:
            f.write(message + "\n")
    def RunReduceTask(self, request, context):
        centroids = request.centroids
        iteration_number=request.iteration_number
        mapper_addresses=request.mapper_addresses
        self.log(f"Reducer {self.reducer_id}: Starting reduce task, iteration {iteration_number}, mapper addresses: {mapper_addresses}")
        key_value_pairs = load_key_value_pairs(mapper_addresses)
        self.log(f"Reducer {self.reducer_id}: Fetched data from mappers") 
        # Group points by centroid ID
        points_by_centroid = {}
        for centroid_id, point in key_value_pairs:
            points_by_centroid.setdefault(centroid_id, []).append(point)

        # Calculate new centroids
        new_centroids = []
        for centroid_id, points in points_by_centroid.items():
            new_centroid = calculate_new_centroid(points=points,centroid_id=centroid_id)
            new_centroids.append(new_centroid)

        # Write new centroids to a file
        with open(f"Reducers/R{reducer_id}.txt", "w") as f:
            for centroid in new_centroids:
                f.write(str(centroid) + "\n")  
            self.log(f"Reducer {self.reducer_id}: Completed reduce task")
        return kmeans_pb2.ReducerResponse(status=kmeans_pb2.ReducerResponse.Status.SUCCESS)

    def GetCentroids(self, request, context):
        os.makedirs("Reducers", exist_ok=True) 
        with open(f"Reducers/R{reducer_id}.txt", "r") as f:
            centroids = [kmeans_pb2.Centroid.FromString(line) for line in f]
        return kmeans_pb2.GetCentroidsResponse(centroids=centroids)


if __name__ == "__main__":
    reducer_id = int(sys.argv[1])
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  
    kmeans_pb2_grpc.add_ReducerServiceServicer_to_server(ReducerServer(), server)
    server.add_insecure_port(f'[::]:{6000 + reducer_id}') 
    server.start()
    server.wait_for_termination()


