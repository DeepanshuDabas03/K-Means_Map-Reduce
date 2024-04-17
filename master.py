
import multiprocessing
import os
import random
import subprocess
import threading
import grpc
import re
import math

import kmeans_pb2  #type:ignore
import kmeans_pb2_grpc #type:ignore


class master(kmeans_pb2_grpc.MapperServiceServicer, kmeans_pb2_grpc.ReducerServiceServicer): # type: ignore
    def __init__(self,mappers,reducers,centroids,iterations):
        self.mappers = mappers # type: ignore
        self.reducers = reducers # type: ignore
        self.centroids = centroids # type: ignore 
        self.iterations = iterations   # type: ignore
        self.dataFile = "Data/input/points.txt" # Location of data file
        self.dumpFile="Data/dump/master_dump.txt"
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
                coordinates = [float(x) for x in line.strip().split(',')]
                points.append(kmeans_pb2.Point(coordinates=coordinates))
 
        return points
    
    def initialize_centroids(self, points, k):
        selected_points = random.sample(points, k) 
        #print(selected_points)
        print()
        centroids = []
        for i, point in enumerate(selected_points, start=1):
            centroid = kmeans_pb2.Centroid(id=i, coordinates=point.coordinates) 
            #print(centroid.id, centroid.coordinates)
            centroids.append(centroid)

        print(centroids)
        return centroids 
    
    def log(self,message):
        os.makedirs("Data/dump", exist_ok=True)
        with open(self.dumpFile, "a") as f:
            f.write(message + "\n")
            
    def writeOutput(centroids):
        with open("Data/centroids.txt", "w") as f:
            for centroid in centroids:
                f.write(f"{','.join(str(x) for x in centroid.coordinates)}\n")

    
    def handlePoints(self, filename):
        with open(filename, "r") as f:
            lines = f.readlines()

        with open(filename, 'w') as file:
            for line in lines:
        # Use regular expression to find commas without a space after them
                modified_line = re.sub(r',([^ ])', r', \1', line)
        # Write the modified line to the file
                file.write(modified_line)


    def calculate_distance(self, c1, c2):
        x1, y1 = c1.coordinates
        x2, y2 = c2.coordinates
        distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
        return distance

    
    def checkEarlyTermination(self, old_centroids, new_centroids):
        epsilon = 1e-6
        count = 0
        for centroid1, centroid2 in zip(old_centroids, new_centroids):
            if self.calculate_distance(centroid1, centroid2) < epsilon:
                count += 1
        return count
    
    
    def start(self):
        self.handlePoints(self.dataFile)
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
                thread = threading.Thread(target=self.start_reducer, args=(i+1, centroids, iteration))
                thread.start()
                reducer_threads.append(thread)

            for thread in reducer_threads:
                thread.join()

            old_centroids = centroids
            centroids = self.compile_centroids()

            self.log("Centroids: ")
            for centroid in centroids:
                self.log(str(centroid))
            for process in self.mappersList:
                process.terminate()
            for process in self.reducersList:
                process.terminate()

            count = self.checkEarlyTermination(old_centroids, centroids)
            if count == len(centroids):
                self.log("Early Termination")
                break

        master.writeOutput(centroids)
        for process in self.mappersList:
            process.terminate()
        for process in self.reducersList:
            process.terminate()
        print("KMeans Clustering Completed")
    
    def restart_worker(self, worker_type, worker_id,centroids,iteration_number,data_split=None):  # Simplified restart
        self.log(f"Re-running failed {worker_type}  {worker_id}")
        if worker_type == "Mapper":
            self.start_mapper(worker_id,data_split=data_split,centroids=centroids,iteration_number=iteration_number) 
        elif worker_type == "Reducer":
            self.start_reducer(worker_id,centroids=centroids,iteration_number=iteration_number)

    def start_mapper(self, mapper_id, data_split, centroids, iteration_number):
        mapper_address = f'localhost:{5000 + mapper_id }'
        try:
            process = subprocess.Popen(["python", "mapper.py", str(mapper_id),str(self.reducers)])
            self.mappersList.append(process)
        except Exception as e:
            self.log(f"Error starting mapper {mapper_id}")
        self.monitor_mapper(mapper_id, process, mapper_address, data_split, centroids, iteration_number)

    def monitor_mapper(self, mapper_id, process, mapper_address, data_split, centroids, iteration_number):
        self.log(f"Sending gRPC RunMapTask request to Mapper {mapper_id}") 
        with grpc.insecure_channel(mapper_address) as channel:
            stub = kmeans_pb2_grpc.MapperServiceStub(channel)
            try:
                response = stub.RunMapTask(kmeans_pb2.MapperRequest(
                    data_split=data_split,
                    centroids=centroids,
                    iteration_number=iteration_number
                ))
                if response.status != kmeans_pb2.MapperResponse.Status.SUCCESS:
                    self.log(f"Mapper {mapper_id}: gRPC RunMapTask response - FAILURE")
                    process.terminate()
                    self.restart_worker("Mapper", mapper_id, centroids, iteration_number, data_split=data_split)
                else:
                    self.log(f"Mapper {mapper_id}: gRPC RunMapTask response - SUCCESS")
            except grpc.RpcError as e:

                self.log(f"FAILURE: Mapper {mapper_id} gRPC Error: {e}")
                process.terminate()
    
    
                self.restart_worker("Mapper", mapper_id,centroids,iteration_number, data_split=data_split)
            finally:
                channel.close()

    def start_reducer(self, reducer_id, centroids, iteration_number):
        reducer_address = f'localhost:{6000 + reducer_id }'  # Reducers on port 6001,6002,6003........
        try:
            process = subprocess.Popen(["python", "reducer.py", str(reducer_id)])
            self.reducersList.append(process)
        except Exception as e:
            self.log(f"Error starting reducer {reducer_id}")
        self.monitor_reducer(reducer_id, process, reducer_address, centroids, iteration_number)

    def monitor_reducer(self, reducer_id, process, reducer_address, centroids, iteration_number):
        self.log(f"Sending gRPC RunReduceTask request to Reducer {reducer_id}")     
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
                    self.log(f"Reducer {reducer_id}: gRPC RunReduceTask response - FAILURE")
                    process.terminate()
                    self.restart_worker("Reducer", reducer_id, centroids, iteration_number)
                else:
                    self.log(f"Reducer {reducer_id}: gRPC RunReduceTask response - SUCCESS")
            except grpc.RpcError as e:
                self.log(f"Reducer {reducer_id} gRPC Error: {e}")
                process.terminate()
                self.restart_worker("Reducer", reducer_id,centroids,iteration_number)
            finally:
                channel.close()
        

    def compile_centroids(self):
        centroids = []
        for i in range(self.reducers):
            reducer_address = f'localhost:{6000 + i + 1}'
            with grpc.insecure_channel(reducer_address) as channel:
                stub = kmeans_pb2_grpc.ReducerServiceStub(channel)
                response = stub.GetCentroids(kmeans_pb2.GetCentroidsRequest())
                centroids.extend(response.centroids)
    
        return centroids


if __name__== "__main__":
    mappers=int(input("Enter the number of mappers: "))
    reducers=int(input("Enter the number of reducers: "))
    centroids=int(input("Enter the number of centroids: "))
    iterations=int(input("Enter the number of iterations: "))
    masterNode = master(mappers=mappers,reducers=reducers,centroids=centroids,iterations=iterations)
    masterNode.start()

    