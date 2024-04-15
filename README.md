**Distributed K-Means Clustering with a Custom MapReduce Framework**

**Overview**

This project implements a distributed K-means clustering algorithm using a custom-built MapReduce framework. It is designed to handle potentially large datasets by distributing the clustering workload across multiple processes or machines.

**Problem Statement**

The project aims to solve the problem of clustering large datasets using the K-means algorithm in a distributed environment. 

**Key Features**

*   **Master Process:** Coordinates the overall MapReduce workflow, including input splitting, process management, and centroid compilation.
*   **Mapper Processes:** Perform data loading, centroid assignment,  and output partitioning.
*   **Reducer Processes:**  Aggregate data points, recalculate centroids, and write new centroids to output files.
*   **gRPC Communication:** Facilitates efficient communication and data exchange among the master, mappers, and reducers.
*   **Fault Tolerance:**  Implements mechanisms to detect and restart failed mapper or reducer processes, ensuring reliable completion.
*   **Convergence Tracking:** Includes the ability to track centroid convergence and terminate the algorithm early if convergence is reached.

**Prerequisites**

*   Python 3.x
*   grpcio, grpcio-tools (`pip install grpcio grpcio-tools`)

**Directory Structure**

```
Data/
    Input/
        points.txt                 # Input data file 
    Mappers/
        M1/ 
            partition_1.txt        # Output from Mapper 1
            partition_2.txt
            ...
        ...
    Reducers/
        R1.txt                     # Output from Reducer 1
        ...
    centroids.txt                  # Final converged centroids
    dump/                          # Log files
master.py
mapper.py
reducer.py
kmeans.proto                       # Protobuf definitions
```

**Running the Code**

1.  **Start Mappers and Reducers:** Launch mapper and reducer processes manually or have the master script spawn them. Ensure that the number of processes matches the provided arguments.

2.  **Execute Master:**
    ```bash
    python master.py <number_of_mappers> <number_of_reducers> <number_of_centroids> <number_of_iterations>
    ```

**Usage Example**

*   **Input:** `Data/Input/points.txt` (See provided sample in the repository for the format).
*   **Command:** `python master.py 3 2 5 10` (3 mappers, 2 reducers, 5 initial centroids, 10 iterations).
*   **Output:** 
    *   `Data/Reducers/R*.txt`: New centroids from each reducer.
    *   `Data/centroids.txt`: Final compiled list of converged centroids.

**Protobuf (kmeans.proto)**

The `kmeans.proto` file defines the data structures and gRPC services used for communication within the system. See the separate README within the proto directory or the generated code for details.

**Logging**

Log files for the master, mappers, and reducers are stored in the `Data/dump/` directory.

**Customization**  

*   **Input Splitting:**  Modify the `master.py` if you wish to implement a different input data-splitting strategy.
*   **Convergence Criteria:**  Adjust the convergence-checking logic if needed.  
