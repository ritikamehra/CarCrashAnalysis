import json

 # Utility Functions

def extract_config(config_filepath):
    ''' This function is used to extract configuration from json file
    Parameter:
        config_filepath: configuration file path
    '''
    with open(config_filepath, "r") as file:
        return json.load(file, strict=False)

def read_file(spark, filepath):
    ''' This function is used to read the csv files and create dataframes
    Parameter:
        filepath: input file path
    '''
    return spark.read.options(header=True, inferSchema=True).csv(filepath)    
        
def write_output_file(df, filepath):
    ''' This function is used to read the csv files and create dataframes
    Parameter:
        df: dataframe with output data
        filepath: filepath of input
    '''
    return df.coalesce(1).write.mode("overwrite").options(header=True).csv(filepath)