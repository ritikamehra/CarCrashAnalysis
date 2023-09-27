''' Importing system dependencies '''
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from utilities import utility

class CarCrashAnalysis:

    def __init__(self):
        ''' This function is used to initialize spark session and input and output path variables'''
        self.spark = SparkSession\
            .builder \
            .master("local[*]") \
            .appName("CarCrashCaseStudy") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        


    def load_df(self, config_filepath):
      ''' This function is used to load data to dataframes, remove duplicates and store common joins  
      Parameter:
            config_filepath : filpath of configfile
      '''

      self.source_filepath = utility.extract_config(config_filepath).get('source_paths')
      self.output_filepath = utility.extract_config(config_filepath).get('output_paths')

      # Loading data to dataframes
      self.charges = utility.read_file(self.spark, self.source_filepath.get('charges'))
      self.damages = utility.read_file(self.spark, self.source_filepath.get('damages'))
      self.endorses = utility.read_file(self.spark, self.source_filepath.get('endorses'))
      self.primaryperson = utility.read_file(self.spark, self.source_filepath.get('primaryperson'))
      self.restricts = utility.read_file(self.spark, self.source_filepath.get('restricts'))
      self.units = utility.read_file(self.spark, self.source_filepath.get('units'))

      # Removing duplicates
      self.units_dist = self.units.dropDuplicates()
      self.damages_dist = self.damages.dropDuplicates()
      self.charges_dist = self.charges.dropDuplicates() 

      # Storing common join      
      self.pp_unit = self.primaryperson.join(self.units_dist,(self.primaryperson["CRASH_ID"] == self.units_dist["CRASH_ID"])  & (self.primaryperson['UNIT_NBR'] == self.units_dist['UNIT_NBR']), "inner")   
    

    # Analysis Functions Begin

    # Analysis 1
    def male_fatal_crashes(self, output_filepath):
        ''' This function is used to calculate number of crashes with male fatalities
        Parameter:
            output_filepath : filpath of output
        '''

        df = self.primaryperson.where((self.primaryperson.PRSN_GNDR_ID == 'MALE') & (self.primaryperson.DEATH_CNT == 1))

        utility.write_output_file(df, output_filepath)

        return df.count()

    # Analysis 2
    def two_wheeler_crashes(self, output_filepath):
        ''' This function is used to calculate number of 2 wheelers booked for crashes
        Parameter:
            output_filepath : filpath of output
        '''
        
        df = self.units_dist.where(self.units_dist.VEH_BODY_STYL_ID.contains('MOTORCYCLE'))

        utility.write_output_file(df, output_filepath)

        return df.count()
    
    # Analysis 3
    def state_high_female_crashes(self, output_filepath):
        ''' This function is used to find the state with highest number of crashes with females involved
        Parameter:
            output_filepath : filpath of output
        '''
        df = self.primaryperson.where(self.primaryperson.PRSN_GNDR_ID =='FEMALE')\
        .groupBy(self.primaryperson.DRVR_LIC_STATE_ID)\
        .agg(count('*').alias('ACCIDENT_COUNT'))\
        .orderBy(col('ACCIDENT_COUNT').desc())\
        .limit(1)

        utility.write_output_file(df, output_filepath)

        return df.collect()[0][0]

    # Analysis 4
    def vehicle_make_high_injuries(self, output_filepath):
        ''' This function is used to find the top 5th to 15th vehicle makes with highest crashes
        Parameter:
            output_filepath : filpath of output
        '''        

        df = self.units_dist.withColumn('TOT_INJRY_DEATH', self.units_dist.TOT_INJRY_CNT + self.units_dist.DEATH_CNT)\
                                        .select('VEH_MAKE_ID','TOT_INJRY_DEATH')\
                                        .where(~col('VEH_MAKE_ID').isin('NA'))\
                                        .groupBy(self.units_dist.VEH_MAKE_ID)\
                                        .agg(sum(col('TOT_INJRY_DEATH')).alias('TOT_INJRY_DEATH'))\
                                        .orderBy(col('TOT_INJRY_DEATH').desc())\
                                        .limit(15)
        
        filter_df = df.where(df.VEH_MAKE_ID.isin([rec[0] for rec in df.collect()[4:]]))
        utility.write_output_file(filter_df, output_filepath)

        return [rec[0] for rec in filter_df.collect()]
    
    # Analysis 5
    def body_style_ethnic_group(self, output_filepath):
        ''' This function is used to find top ethnic groups for each vehicle body style 
        Parameter:
            output_filepath : filpath of output
        '''
        window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('COUNT').desc())
        
        df = self.pp_unit.where(~self.pp_unit['VEH_BODY_STYL_ID'].isin(['NA', 'UNKNOWN', 'NOT REPORTED']))\
        .where(~self.pp_unit['PRSN_ETHNICITY_ID'].isin(['NA', 'UNKNOWN']))\
        .groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').agg(count('PRSN_ETHNICITY_ID').alias('COUNT'))\
        .withColumn('RANK', dense_rank().over(window))\
        .where(col('RANK') == 1).select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')\
        .orderBy('VEH_BODY_STYL_ID')

        utility.write_output_file(df, output_filepath)

        df.show(truncate=False)
    
    # Analysis 6
    def zipcode_high_crash(self, output_filepath):
        ''' This function is used to find top 5 Zip Codes with highest number crashes with alcohols as the contributing factor
        Parameter:
            output_filepath : filpath of output
        '''
        zipcodeDf = self.pp_unit.where(col('CONTRIB_FACTR_1_ID').contains('ALCOHOL') | col('CONTRIB_FACTR_2_ID').contains('ALCOHOL') |col('CONTRIB_FACTR_P1_ID').contains('ALCOHOL'))\
        .where(~col('DRVR_ZIP').isin('NONE'))\
        .select(self.units.CRASH_ID,'DRVR_ZIP')
        df = zipcodeDf.groupBy(col('DRVR_ZIP'))\
        .agg(count(col('CRASH_ID')).alias('CRASH_COUNT'))\
        .orderBy(col('CRASH_COUNT').desc())\
        .limit(5)

        utility.write_output_file(df, output_filepath)

        return [rec[0] for rec in df.collect()]

    # Analysis 7
    def no_prop_dam_car_insur(self, output_filepath):
        ''' This function is used to find count of distinct crash ids where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        Parameter:
            output_filepath : filpath of output
        '''
        
        damagesUnitsDf = self.units_dist.join(self.damages_dist,self.units_dist['CRASH_ID'] == self.damages_dist['CRASH_ID'], how = 'inner')\
        .select(self.units_dist.CRASH_ID, 'DAMAGED_PROPERTY', 'VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID', 'FIN_RESP_TYPE_ID')
        
        df = damagesUnitsDf.where(col('DAMAGED_PROPERTY') == 'NONE')\
        .where((col('VEH_DMAG_SCL_1_ID').isin('DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 Highest')) | (col('VEH_DMAG_SCL_2_ID').isin('DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 Highest')))\
        .where(col('FIN_RESP_TYPE_ID') != 'NA').select(self.units_dist.CRASH_ID).distinct()


        utility.write_output_file(df, output_filepath)

        return df.count()


    # Analysis 8
    def vehc_make_speed(self, output_filepath):
        ''' This function is used to determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences has licensed Drivers,
        used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        Parameter:
            output_filepath : filpath of output        '''
                       
        top_ten_col = [rec[0] for rec in self.units_dist.groupBy(self.units_dist.VEH_COLOR_ID).agg(count('*').alias('COUNT')).orderBy(col('COUNT').desc()).limit(5).collect()]
        top_25_state = [rec[0] for rec in self.units_dist.groupBy(self.units_dist.VEH_LIC_STATE_ID).agg(count('*').alias('COUNT')).orderBy(col('COUNT').desc()).limit(25).collect()]
        
        df = self.pp_unit.join(self.charges_dist, (self.charges_dist["CRASH_ID"] == self.units["CRASH_ID"]) & (self.charges_dist["UNIT_NBR"] == self.units["UNIT_NBR"]), how ="inner")\
        .where(col('CHARGE').contains('SPEED'))\
        .where(~col('DRVR_LIC_TYPE_ID').isin('NA','UNKNOWN','OTHER','UNLICENSED'))\
        .where(col('VEH_COLOR_ID').isin(top_ten_col))\
        .where(col('VEH_LIC_STATE_ID').isin(top_25_state))\
        .groupBy(col('VEH_MAKE_ID')).agg(count('*').alias('COUNT'))\
        .orderBy(col('COUNT').desc()).limit(5)
        
        utility.write_output_file(df, output_filepath)

        return df.show()

    def analysis_handler(self):
        ''' This function is used to call the functions for analysis which will print the output on console and write in csv files.'''

        # Analysis 1
        print('Analysis 1: \nNumber of crashes with male fatalities: ', self.male_fatal_crashes(self.output_filepath.get('Analysis1')))

        # Analysis 2
        print('\nAnalysis 2: \nNumber of two wheeler crashes: ', self.two_wheeler_crashes(self.output_filepath.get('Analysis2')))

        # Analysis 3
        print('\nAnalysis 3: \nThe state with highest number of crashes with females involved : ',self.state_high_female_crashes(self.output_filepath.get('Analysis3')))

        # Analysis 4
        print('\nAnalysis 4: \nTop 5th to 15th vehicle makes with highest crashes: ', self.vehicle_make_high_injuries(self.output_filepath.get('Analysis4')))

        # Analysis 5
        print('\nAnalysis 5: \nTop ethnic groups for each vehicle body style: ')
        self.body_style_ethnic_group(self.output_filepath.get('Analysis5'))

        # Analysis 6
        print('\nAnalysis 6: \nTop 5 Zip Codes with highest number crashes with alcohols as the contributing factor: ', self.zipcode_high_crash(self.output_filepath.get('Analysis6')))

        # Analysis 7
        print('\nAnalysis 7: \nCount of distinct crash ids where No Damaged Property was observed, Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance: ',self.no_prop_dam_car_insur(self.output_filepath.get('Analysis7')))
        
         # Analysis 8
        print('\nAnalysis 8: \nTop 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences: ')
        self.vehc_make_speed(self.output_filepath.get('Analysis8'))

        
        self.spark.stop()

if __name__ == "__main__":

   
    # Initializing class object
    car_crash_obj = CarCrashAnalysis()

    config_filepath = 'config.json'

   # Calling function to load data to dataframes
    car_crash_obj.load_df(config_filepath)

    # Calling function to perform analysis
    car_crash_obj.analysis_handler()


                                                                                                                                                                                  
