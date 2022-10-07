import datetime
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from utils import Configs

holidays = Configs().global_configs['date_time_params']['holidays']


class Preprocess:
    shifted_features_dict = {
            # shift search day -1
            "min_fare_prev_search_day": {
                'sort_col': 'searchDt',
                'groupby_cols': ['outDeptDt', 'inDeptDt']
            },
            # shift departure day +/- 1
            "min_fare_prev_dept_day": {
                'sort_col': 'outDeptDt',
                'groupby_cols': ['inDeptDt', 'searchDt']
            },
            "min_fare_next_dept_day": {
                'sort_col': 'outDeptDt',
                'groupby_cols': ['inDeptDt', 'searchDt'],
                'shift': -1
            },
            # shift return day +/- 1
            "min_fare_prev_return_day": {
                'sort_col': 'inDeptDt',
                'groupby_cols': ['outDeptDt', 'searchDt'],
            },
            "min_fare_next_return_day": {
                'sort_col': 'inDeptDt',
                'groupby_cols': ['outDeptDt', 'searchDt'],
                'shift': -1
            }
        }

    def __init__(self, 
                 test_size,
                 target_col="min_fare",
                 trail_avg_window=3,
                 fill_na_strategy='avg',
                 clip_first_shop_day=True,
                 first_shop_date=None,
                 holidays=holidays,
                 seed=19,
                 *args):
        """
        params:
        ----------
        trailing_avg_window (int): window size for trailing average
            used as secondary method for fillna of shifted features
        
        fill_na_strategy (str, ('avg', <col>)): strategy to take
            for filling na for trailing avg. 'avg' = take average
            of all shifted features. <col> = specify column name
            of shifted feature to use.
        
        first_shop_date (datetime or None): first date of shopping
            in data. Only need to supply when `clip_first_shop_day`
            is True. Default=None.
        """
        self.args = args
        self.test_size = test_size
        self.target_col = target_col
        self.seed = seed
        self.feature_list = []
        self.trail_avg_window = trail_avg_window
        self.fill_na_strategy = fill_na_strategy
        self.clip_first_search_day = clip_first_shop_day
        self.first_search_dt = first_shop_date
        self.holidays = holidays
        self._masking = False
        self._dow_added = False
        
        
    def mask(self, X, y):
        """Train-test split X & y, and mask y_test in recombined dataset"""
        X_train, X_test, y_train, self.y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.seed)
        train_pdf = pd.concat([X_train, y_train], axis=1)
        train_pdf["train_test"] = "train"
        X_test["train_test"] = "test"
        mask_df = pd.concat([train_pdf, X_test], axis=0, sort=False)
        self.mask_df = mask_df
        self._masking = True
    
    def mask_only(self, X_train, X_test, y_train, y_test):
        """Mask y_test in recombined dataset.
        
        Use if X & y have already been train-test split, such as 
        during cross-validation.
        """
        self.y_test = y_test
        train_pdf = pd.concat([X_train, y_train], axis=1)
        train_pdf["train_test"] = "train"
        X_test["train_test"] = "test"
        mask_df = pd.concat([train_pdf, X_test], axis=0, sort=False)
        self.mask_df = mask_df
        self._masking = True
    
    def feature_eng_shifted(self, df):
        # Don't hard-code which df to operate on, because order of feature
        # engineering doesn't matter
        assert self._masking, "You should only engineers features after masking"
        for feature_name, kwargs in self.shifted_features_dict.items():
            df[feature_name] = (df
                            .sort_values(by=kwargs['sort_col'])
                            .groupby(kwargs['groupby_cols'])[self.target_col]
                            # could also supply fillna value here, if it were simple
                            .shift(kwargs.get("shift", 1)) 
                        )
            self.feature_list.append(feature_name)
        return df
    
    def feature_eng_trailing_avg(self, df):
        # don't hard-code which df. See above reason
        assert self._masking, "You should only engineers features after masking"
        
        if not self._dow_added:
            self.extract_dow(df)

        df['trail_avg'] = (df
                .sort_values(by="searchDt")
                # this is going to be very restrictive. We won't have a lot of data.
                # i.e. will only have a datapoint for a given combination of dtd +
                # dept DOW once every 7 shopping days. 
                # Also need to check that this isn't leaky -- confirm that current
                # data point isn't included
                .groupby(["days_til_dept", "dept_dt_dow", "return_dt_dow"])
                [self.target_col]
                .transform(lambda x: x.rolling(
                    window=self.trail_avg_window, min_periods=1).mean())
               )
        self.feature_list.append("trail_avg")
        
        return df
    
    def feature_eng_fillna(self, df):
        assert 'trail_avg' in df.columns, "Trailing average must be computed before filling na"
        # as a first pass, take average across shifted features 
        # and use this to fillna
        shifted_feature_cols = list(self.shifted_features_dict.keys())
        for col in shifted_feature_cols:
            df[col] = np.where(df[col].isna(),
                               np.nanmean(df[shifted_feature_cols], axis=1),
                               df[col])

        # for the few rows where all shifted features are NaN, use 
        # the trailing average of min_fare to fillna
        for col in shifted_feature_cols:
            df[col] = np.where(df[col].isna(), 
                                df['trail_avg'],
                                df[col])
        
        # finally, where trail_avg is null, use shifted features
        if self.fill_na_strategy == 'avg':
            # take average of all shifted features
            fillna_val = np.nanmean(df[shifted_feature_cols], axis=1)
        else:
            # use column specified
            fillna_val = df[self.fill_na_strategy]
            
        df['trail_avg'] = np.where(df['trail_avg'].isna(),
                                   fillna_val,
                                   df['trail_avg']
                                  )
        
        # drop any remaining NaN's for now
        df_drop = df.dropna(subset=self.feature_list)
        
        x, y = len(df_drop), len(df)
        if x != y:
            print(f"Dropping {y-x} na rows")
        
        return df_drop
    
    
    def extract_dow(self, df):
        df['dept_dt_dow'] = df['outDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
        df['return_dt_dow'] = df['inDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
        self._dow_added = True
        return df
                       
    def feature_eng_dow_holidays(self, df):
        # add "is weekend or holiday" ("is_wkehol") indicators
        # TODO: update these, now that we have `_dow` columns?
        df['dept_dt_is_wkehol'] = df['outDeptDt_dt'].apply(
            lambda d: ((datetime.date.weekday(d) >= 5) | (d in self.holidays)))

        df['dept_dt_prior_is_wkehol'] = df['outDeptDt_dt'].apply(
            lambda d: (datetime.date.weekday(d - datetime.timedelta(days=1)) >= 5)
                        | (d - datetime.timedelta(days=1) in self.holidays))
        df['dept_dt_next_is_wkehol'] = df['outDeptDt_dt'].apply(
            lambda d: (datetime.date.weekday(d + datetime.timedelta(days=1)) >= 5)
                        | (d + datetime.timedelta(days=1) in self.holidays))

        df['return_dt_is_wkehol'] = df['inDeptDt_dt'].apply(
            lambda d: (datetime.date.weekday(d) >= 5) | (d in self.holidays))
        df['return_dt_prior_is_wkehol'] = df['inDeptDt_dt'].apply(
            lambda d: (datetime.date.weekday(d - datetime.timedelta(days=1)) >= 5)
                        | (d - datetime.timedelta(days=1) in self.holidays))
        df['return_dt_next_is_wkehol'] = df['inDeptDt_dt'].apply(
            lambda d: (datetime.date.weekday(d + datetime.timedelta(days=1)) >= 5)
                        | (d + datetime.timedelta(days=1) in self.holidays))
        
        self.feature_list.extend([
            'dept_dt_is_wkehol', 'dept_dt_prior_is_wkehol', 'dept_dt_next_is_wkehol',
            'return_dt_is_wkehol', 'return_dt_prior_is_wkehol', 'return_dt_next_is_wkehol'
        ])
        
        return df

    def resplit(self, df):
        """Re-split data into train & test.
        
        Also optionally clips off first search day of data.
        Intended to be run after all feature engineering steps.
        """
            
        train_df = df[df['train_test'] == 'train']
        test_df = df[df['train_test'] == 'test']
        
        if self.clip_first_search_day:
            # can't just clip the full df -- need to update y_test as well
            
            # pull y_test back into test_df to associate it with search dates
            test_df.loc[:, self.target_col] = self.y_test

            # filter test_df on search date
            test_df = test_df[test_df['searchDt_dt'] > self.first_search_dt]
            # update y_test
            self.y_test = test_df[self.target_col]
            
            train_df = train_df[train_df['searchDt_dt'] > self.first_search_dt]
            
        X_train = train_df.drop(columns=[self.target_col])
        y_train = train_df[self.target_col]
        X_test = test_df.drop(columns=[self.target_col])
        return X_train, X_test, y_train
    
    def processs(self, X, y):
        self.mask(X, y)
        eng_df = self.feature_eng_shifted(self.mask_df)
        eng_df = self.feature_eng_trailing_avg(eng_df)
        eng_df = self.feature_eng_dow_holidays(eng_df)
        fillna_df = self.feature_eng_fillna(eng_df)
        self.preproc_df = fillna_df
        X_train, X_test, y_train = self.resplit(fillna_df)
        return X_train, X_test, y_train, self.y_test
    
    def processs_for_cv(self, X_train, X_test, y_train, y_test):
        self.mask_only(X_train, X_test, y_train, y_test)
        eng_df = self.feature_eng_shifted(self.mask_df)
        eng_df = self.feature_eng_trailing_avg(eng_df)
        eng_df = self.feature_eng_dow_holidays(eng_df)
        fillna_df = self.feature_eng_fillna(eng_df)
        self.preproc_df = fillna_df
        X_train, X_test, y_train = self.resplit(fillna_df)
        return X_train, X_test, y_train, self.y_test
