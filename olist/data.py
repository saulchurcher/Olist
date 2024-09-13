import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities

        # In order to get the file destination with csv files
        csv_path = os.path.join(os.path.dirname((os.path.dirname(__file__))), 'data', 'csv')

        # Getting all csv files from directory
        file_names = [i for i in os.listdir(csv_path) if i.endswith('.csv')]

        # Cleaning file names
        key_names = [f.replace('_dataset.csv','').replace('olist_','').replace('.csv','') for f in file_names]

        # Sorting names with corresponding dataframes into a dictionary
        data = {}
        for key, file in zip(key_names, file_names):
            data[key] = pd.read_csv(os.path.join(csv_path, file))

        return data

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
