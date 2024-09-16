import configparser
from pyspark import SparkConf

# loading the application configs in python dictionary
import configparser

# Loading the application configs in a Python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read('configs/application.config') 
    
    app_conf = {}
    
    # Iterate through all items in the given environment section
    for (key, val) in config.items(env):
        app_conf[key] = val
    
    # Debugging - Print the final configuration after the loop
    # print(f"Config for {env}: {list(app_conf.keys())}")
    
    return app_conf  # Now returns after all items are added


# loading the pyspark configs and creating a spark conf object
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read('configs/pyspark.config') 
    pyspark_conf = SparkConf()
    for (key,val) in config.items(env):
        pyspark_conf.set(key,val)
    return pyspark_conf