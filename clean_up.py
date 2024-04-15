import os
import shutil
os.makedirs(f"Data/dump", exist_ok=True)
shutil.rmtree("Data/dump")
os.makedirs(f"Data/Reducers", exist_ok=True)
shutil.rmtree("Data/Reducers")
os.makedirs("Data/Mappers", exist_ok=True)
shutil.rmtree("Data/Mappers")


