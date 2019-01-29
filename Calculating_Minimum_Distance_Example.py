# -*- coding: utf-8 -*-
"""
Created on Mon Jan 28 18:54:15 2019
@author: Garrett.R
"""

import pandas as pd
import numpy as np
import math

# Creating function to perform a cross join between two dataframes. The cross
# join function produces a cartesian product of both data frames as a result.
def CrossJoin(dfOne,dfTwo):
    """
    This function will create a cartesian product of two dataframe inputs.
    Additional join constraints can be added if user would like to limit the
    amount of cross joined results. However, 'key' is required for function to 
    work correctly.
    :param dfOne is the primary dataframe.
    :param dfTwo is the secondary dataframe to join to primary dataframe.
    """
    return (dfOne.assign(key = 1).merge(dfTwo.assign(key = 1), left_on = ['key'],
                        right_on = ['key']).drop('key',axis = 1))
    
# Creating distance function to perform the strait line distance calculations
# on each line in a cross joined dataset.
def DistanceCalc(find, search):
    """
    DistanceCalc calculates the distance between twho points on earth.
    The distance is measured in miles, but can be changed to km by changing
    the value of R to a radius of 6371 (earths radius).
    :param find is the start point in tuple form (lat, long)
    :param search is end point in tuple form (lat, long)
    :return distance between find and search map points as a float
    """
    R = 3959.8728 #Earth's radius in miles
    find = [math.radians(v) for v in find]
    search = [math.radians(v) for v in search]
    d_lat = search[0] - find[0]
    d_lng = search[1] - find[1]
    a = math.pow(math.sin(d_lat/2),2)+math.cos(find[0])*math.cos(search[0])*math.pow(math.sin(d_lng/2),2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# Creating sample 'find' dataset for this example.
to_find = pd.DataFrame({'source_nbr' : pd.Series(['FindNbr1','FindNbr2','FindNbr3','FindNbr4']),
                        'find_lat' : pd.Series([35.334935,35.335751,35.336142,35.334942]),
                        'find_long' : pd.Series([-119.044910,-119.044618,-119.047413,-119.050253])})

# Creating random subset of numbers for 'score' field in 'search' dataset.
search_score = np.random.randint(1,high = 30, size = 8)

# Creating sample 'search' dataset for this example.
to_search = pd.DataFrame({'source_nbr' : pd.Series(['SearchNbr1','SearchNbr2','SearchNbr3','SearchNbr4',
                                                    'SearchNbr5','SearchNbr6','SearchNbr7','SearchNbr8']),
                          'search_lat' : pd.Series([35.329618,35.329693,35.328460,35.328444,
                                                    35.328472,35.334889,35.342925,35.351744]),
                          'search_long' : pd.Series([-119.053035,-119.050704,-119.051331,-119.047129,
                                                     -119.040212,-119.027727,-119.027019,-119.033653]),
                          'search_score' : pd.Series(search_score)})


# Applying cross joun function to dataframes that were created for this example.
crossJoinedData = CrossJoin(to_find, to_search)

# Examining the crossjoined data set results to verify that data crossjoined correctly.
crossJoinedData.head(3)

# Applying the DistanceCalc function to every row in newly cross joined dataset
# DistanceCalc results will be added to dataframe in column.
crossJoinedData['Calculated_Distance'] = crossJoinedData.apply(lambda row: DistanceCalc((row['find_lat'], row['find_long']),
               (row['search_lat'], row['search_long'])), axis = 1)

# Checking results of applying the DistanceCalc function to dataset.
crossJoinedData.head(3)

# Now selecting to results for each point in the to_find data set by keeping
# only the shortest calculated distance.
crossJoinedData_minDist = crossJoinedData.loc[crossJoinedData.groupby('source_nbr_x')['Calculated_Distance'].idxmin()]

# Now that the un-scored points have been scored, I will clean up the data set
# and remove the columns added from the cross join. After cleanup, will merge
# both to_find and to_search datasets together and create a flag so source of score
# value can be distinguised between 'had score' and 'calculated score'

# Dropping columns that aren't needed anymore in the 'to_find' data set.
to_find_done = crossJoinedData_minDist.drop(['source_nbr_y','search_lat','search_long','Calculated_Distance'], axis = 1)

# Renaming columns in to_find_done dataset.
to_find_done = to_find_done.rename(index = str, columns = {'source_nbr_x':'source_nbr','find_lat':'lat',
                                                           'find_long':'long','search_score':'score'})
# Adding column with flag to indicate that score value was from calculation and not provided.
to_find_done['score_calcd'] = 'True'

# Renaming columns in to_search dataset.
to_search = to_search.rename(index = str, columns = {'search_lat':'lat','search_long':'long',
                                                     'search_score':'score'})
# Adding same column with flag to indicated that score value was provided and not calculated.
to_search['score_calcd'] = 'False'

# Merging both datasets into one so it can be exported to csv to be used other software such
# as Tableau for mapping points, or so dataset can be used in other types of models or analysis
# in python or R, etc.
complete_data = pd.concat([to_find_done, to_search])




