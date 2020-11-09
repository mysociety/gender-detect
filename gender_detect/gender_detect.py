"""
Adaption of approach from:
https://github.com/OpenGenderTracking/globalnamedata

To work quickly with pandas dataframes for fast lookup of
names to gender.
Underlying datasets have been reprocessed and updated.
"""
import os

import numpy as np
import pandas as pd
from scipy.stats import binom


class GenderDetect(object):
    """
    Use official statistics information on births
    to infer gender
    """
    storage_folder = os.path.dirname(__file__)
    prep_signature = "lowerdepun"
    z = 1.96
    male_value = "male"
    female_value = "female"
    unknown_value = "unknown"

    def __init__(self, country="uk_plus", threshold=0.95, lower_threshold=0.75):
        """
        threshold - the proportion of names that have to be the same for a match
        lower_threshold - safety setting for small counts based on binomial distribution
        """
        self.country = country
        self.threshold = threshold
        self.lower_threshold = lower_threshold

        # make sure underlying files exist
        self.__class__.prepare_source_data(self.country)

        # load pickle version of the file
        pickle_storage = self.__class__.calc_data_file_pickle(self.country)
        df = pd.read_pickle(pickle_storage)
        df = df[df.winner_proportion > threshold]
        df = df[df.lower > lower_threshold]
        df = df.set_index("name")
        self.lookup = df["predicted"].to_dict()

    def process_series(self, series, prepare=True):
        """
        Given a series of first names, returns
        a series of gender

        if `prepare` is false, assume that series
        has previously been coerced to lowercase
        and had puntuation removed
        """
        if prepare:
            series = self.prepare_series(series)
        results = series.map(self.lookup)
        results = results.replace({"M": self.__class__.male_value,
                                   "F": self.__class__.female_value,
                                   np.nan: self.__class__.unknown_value})
        return results

    @classmethod
    def prepare_series(cls, series):
        """
        coerce to lower case and remove puntuation
        override to change the convergence method
        """
        series = series.str.lower()
        series = series.str.replace(r'[^\w\s]', '')
        return series

    @classmethod
    def raw_data_file(cls, country="uk_plus"):
        raw_file = os.path.join(cls.storage_folder,
                                "assets",
                                "processed",
                                "{country}_all_time.csv")
        return raw_file.format(country=country)

    @classmethod
    def tidy_data_file(cls, country="uk_plus"):
        tidy_file = os.path.join(cls.storage_folder,
                                 "assets",
                                 "processed",
                                 "{country}_normal_{spec}.csv")
        return tidy_file.format(country=country,
                                spec=cls.prep_signature)

    @classmethod
    def calc_data_file(cls, country="uk_plus"):
        tidy_file = os.path.join(cls.storage_folder,
                                 "assets",
                                 "processed",
                                 "{country}_normal_{spec}_calc.csv")
        return tidy_file.format(country=country,
                                spec=cls.prep_signature)

    @classmethod
    def calc_data_file_pickle(cls, country="uk_plus"):
        tidy_file = os.path.join(cls.storage_folder,
                                 "assets",
                                 "processed",
                                 "{country}_normal_{spec}_calc.pickle")
        return tidy_file.format(country=country,
                                spec=cls.prep_signature)

    @classmethod
    def tidy_data(cls, country="uk_plus"):
        """
        apply transformation function to source names
        """
        df = pd.read_csv(cls.raw_data_file(country))
        df["name"] = cls.prepare_series(df["name"])

        # merge any names that the function now converges on the same key

        df = df.pivot_table(["F", "M"], 'name', aggfunc='sum', fill_value=0)
        df = df.reset_index()
        df.to_csv(cls.tidy_data_file(country), index=False)

    @classmethod
    def calculate_gender(cls, country="uk_plus"):
        """
        apply transformation function to source names
        """
        types = {"name": "string", "F": "int", "M": "int", "source": "string"}
        df = pd.read_csv(cls.tidy_data_file(country), dtype=types)

        # calculate relative proportions
        df["total"] = df.M + df.F
        df["f_proportion"] = df.F / df.total
        df["m_proportion"] = df.M / df.total

        # find winner
        df["winner"] = df[["M", "F"]].max(axis="columns")
        df["winner_proportion"] = df.winner / df.total

        # set predicted value
        df["predicted"] = "M"
        df.loc[df["F"] == df["winner"], "predicted"] = "F"

        # calculate lower bar
        variance = df.apply(lambda x: binom.var(x.total, x.winner_proportion),
                            axis="columns")
        df["lower"] = (df.winner - (np.sqrt(variance) * cls.z)) / df.total

        df.to_csv(cls.calc_data_file(country), index=False)

    @classmethod
    def create_source_pickle(cls, country="uk_plus"):
        """
        create a local pickle cache of the data after first run
        """
        df = pd.read_csv(cls.calc_data_file(country))
        df = df[["name", "winner_proportion", "predicted", "lower"]]
        df.to_pickle(cls.calc_data_file_pickle(country))

    @classmethod
    def prepare_source_data(cls, country="uk_plus", force=False):
        """
        ensure processed source files exist for specified country
        """
        if os.path.exists(cls.raw_data_file(country)) is False:
            message = "Source file for {country} missing"
            raise ValueError(message.format(country=country))
        if os.path.exists(cls.tidy_data_file(country)) is False or force:
            print("Tidying dataset by {0}".format(cls.prep_signature))
            cls.tidy_data(country)
        if os.path.exists(cls.calc_data_file(country)) is False or force:
            print("Calculating gender ranges")
            cls.calculate_gender(country)
        if os.path.exists(cls.calc_data_file_pickle(country)) is False or force:
            print("Creating reduced pickle store of data")
            cls.create_source_pickle(country)