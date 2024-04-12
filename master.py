
import os
import random
import subprocess
import threading
import grpc

import kmeans_pb2  #type:ignore
import kmeans_pb2_grpc #type:ignore


class master(kmeans_pb2_grpc.MapperServiceServicer, kmeans_pb2_grpc.ReducerServiceServicer): # type: ignore
    def __init__(self,mappers,reducers,centroids,iterations):
        self.mappers = mappers # type: ignore
        self.reducers = reducers # type: ignore
        self.centroids = centroids # type: ignore 
        self.iterations = iterations   # type: ignore
        self.dataFile = "input/points.txt" # Location of data file
        self.dumpFile="dump/master_dump.txt"
        self.mappersList=[]
        self.reducersList=[]
        self.WORKER_TIMEOUT = 30  # Timeout for worker processes
    

    def inputSplit(self,dataPoints):
        splits = []
        with open(self.dataFile, 'r') as f:
            lines_per_split = dataPoints // self.mappers 
            current_split_start = 0
            current_split_end = lines_per_split 

            for i in range(self.mappers):
                # if last mapper, read all remaining lines
                if i == self.mappers - 1:  
                    current_split_end = dataPoints 
                splits.append((current_split_start, current_split_end)) 
                current_split_start = current_split_end 
                current_split_end += lines_per_split
        return splits
    
    def load_data(self,filename):
        points = []
        with open(filename, "r") as f:
            for line in f:
                coordinates = [float(x) for x in line.strip().split(', ')]
                points.append(kmeans_pb2.Point(coordinates=coordinates))
        return points
    
    def initialize_centroids(self,points, k):
        return random.sample(points, k)
    
    def log(self,message):
        os.makedirs("dump", exist_ok=True)
        with open(self.dumpFile, "a") as f:
            f.write(message + "\n")

    def start(self):
        points = self.load_data(self.dataFile)
        centroids = self.initialize_centroids(points, self.centroids)
        self.log("Centroids (Initial):")  # Log initial centroids
        for centroid in centroids:
            self.log(str(centroid))
        for iteration in range(1, self.iterations + 1):
            self.log(f"Iteration: {iteration}")

            data_splits = self.inputSplit(len(points))
            mapper_threads = []

            for i, split in enumerate(data_splits):
                thread = threading.Thread(target=self.start_mapper, args=(i+1, f"{split[0]}-{split[1]}", centroids, iteration))
                thread.start()
                mapper_threads.append(thread)

            for thread in mapper_threads:
                thread.join()

            reducer_threads = []
            for i in range(self.reducers):
                thread = threading.Thread(target=self.start_reducer, args=(i, centroids, iteration))
                thread.start()
                reducer_threads.append(thread)

            for thread in reducer_threads:
                thread.join()

            centroids = self.compile_centroids()

            self.log("Centroids: ")
            for centroid in centroids:
                self.log(str(centroid))

    def restart_worker(self, worker_type, worker_id):  # Simplified restart
        self.log(f"Restarting failed {worker_type}  {worker_id}")
        if worker_type == "Mapper":
            self.start_mapper(worker_id) 
        elif worker_type == "Reducer":
            self.start_reducer(worker_id)

    def start_mapper(self, mapper_id, data_split, centroids, iteration_number):
        mapper_address = f'localhost:{5000 + mapper_id + 1}'
        process = subprocess.Popen(["python", "mapper/mapper.py", str(mapper_id),str(self.reducers)])

        self.monitor_mapper(mapper_id, process, mapper_address, data_split, centroids, iteration_number)

    def monitor_mapper(self, mapper_id, process, mapper_address, data_split, centroids, iteration_number):
        try:
            process.wait(timeout=self.WORKER_TIMEOUT)  # Wait for mapper to complete
        except subprocess.TimeoutExpired:
            self.restart_worker("Mapper", mapper_id)
            return

        # gRPC communication after process is ready
        with grpc.insecure_channel(mapper_address) as channel:
            stub = kmeans_pb2_grpc.MapperServiceStub(channel)
            try:
                response = stub.RunMapTask(kmeans_pb2.MapperRequest(
                    data_split=data_split,
                    centroids=centroids,
                    iteration_number=iteration_number
                ))
                if response.status != kmeans_pb2.MapperResponse.Status.SUCCESS:
                    self.restart_worker("Mapper", mapper_id)
                    self.log(f"Mapper {mapper_id}: gRPC RunMapTask response - FAILURE")
                else:
                    self.log(f"Mapper {mapper_id}: gRPC RunMapTask response - SUCCESS")
            except grpc.RpcError as e:
                self.log(f"Mapper {mapper_id} gRPC Error: {e}")
                self.restart_worker("Mapper", mapper_id)

    def start_reducer(self, reducer_id, centroids, iteration_number):
        reducer_address = f'localhost:{6000 + reducer_id + 1}'  # Reducers on port 6001,6002,6003........
        process = subprocess.Popen(["python", "reducer/reducer.py", str(reducer_id)])

        self.monitor_reducer(reducer_id, process, reducer_address, centroids, iteration_number)

    def monitor_reducer(self, reducer_id, process, reducer_address, centroids, iteration_number):
        try:
            process.wait(timeout=self.WORKER_TIMEOUT)  # Wait for reducer to complete
        except subprocess.TimeoutExpired:
            self.restart_worker("Reducer", reducer_id)
            return
        mapper_addresses = [f'localhost:{5000 + i + 1}' for i in range(self.mappers)]
        # gRPC communication after process is ready
        with grpc.insecure_channel(reducer_address) as channel:
            stub = kmeans_pb2_grpc.ReducerServiceStub(channel)
            try:
                response = stub.RunReduceTask(kmeans_pb2.ReducerRequest(
                    centroids=centroids,
                    iteration_number=iteration_number,
                    mapper_addresses=mapper_addresses
                ))
                if response.status != kmeans_pb2.ReducerResponse.Status.SUCCESS:
                    self.restart_worker("Reducer", reducer_id)
                    self.log(f"Reducer {reducer_id}: gRPC RunReduceTask response - FAILURE")
                else:
                    self.log(f"Reducer {reducer_id}: gRPC RunReduceTask response - SUCCESS")
            except grpc.RpcError as e:
                self.log(f"Reducer {reducer_id} gRPC Error: {e}")
                self.restart_worker("Reducer", reducer_id)

    def compile_centroids(self):
        centroids = []
        for i in range(self.reducers):
            reducer_address = f'localhost:{6000 + i + 1}'
            with grpc.insecure_channel(reducer_address) as channel:
                stub = kmeans_pb2_grpc.ReducerServiceStub(channel)
                response = stub.GetCentroids(kmeans_pb2.GetCentroidsRequest)
                centroids.extend(response.centroids)
        return centroids


if __name__== "__main__":
    mappers=int(input("Enter the number of mappers: "))
    reducers=int(input("Enter the number of reducers: "))
    centroids=int(input("Enter the number of centroids: "))
    iterations=int(input("Enter the number of iterations: "))
    masterNode = master(mappers=mappers,reducers=reducers,centroids=centroids,iterations=iterations)
    masterNode.start()

    