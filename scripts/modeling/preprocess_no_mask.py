"""

Version notes:
- branched off of preprocess on 10/03/2022
- added in "generic" trailing average calc method
    and trailing_features_dict
"""
import datetime
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from utils import Configs

holidays = Configs().global_configs['date_time_params']['holidays']


class Preprocess:
    shifted_features_dict = {
            # shift search day -1
            "prev_search_day": {
                'sort_col': 'searchDt',
                'groupby_cols': ['outDeptDt', 'inDeptDt']
            },
            
            # Not sure if these provide much value.
            # Could consider constructing them as a sort of
            # trailing avg. 

            # # shift departure day +/- 1
            # "min_fare_prev_dept_day": {
            #     'sort_col': 'outDeptDt',
            #     'groupby_cols': ['inDeptDt', 'searchDt']
            # },
            # "min_fare_next_dept_day": {
            #     'sort_col': 'outDeptDt',
            #     'groupby_cols': ['inDeptDt', 'searchDt'],
            #     'shift': -1
            # },
            # # shift return day +/- 1
            # "min_fare_prev_return_day": {
            #     'sort_col': 'inDeptDt',
            #     'groupby_cols': ['outDeptDt', 'searchDt'],
            # },
            # "min_fare_next_return_day": {
            #     'sort_col': 'inDeptDt',
            #     'groupby_cols': ['outDeptDt', 'searchDt'],
            #     'shift': -1
            # }
        }
    
    trailing_features_dict = {
        "ta_over_dows": {
            "groupby_cols": ['dept_dt_dow', 'return_dt_dow'],
            "window": [1, 3, 7]
        },
        "ta_over_dtd_los": {
            "groupby_cols": ['days_til_dept', 'stay_duration'],
            "window": [1, 3, 7]
        },
        "ta_over_travel_dts": {
            "groupby_cols": ['outDeptDt', 'inDeptDt'],
            "window": [1, 3, 7]
        },
        "ta_dtd": {
            "groupby_cols": ['days_til_dept'],
            "window": [1, 3, 7]
        }
    }

    def __init__(self, 
                 target_col="min_fare",
                 holidays=holidays,
                #  seed=19,
                 *args):
        """
        params:
        ----------        

        """
        self.args = args
        self.target_col = target_col
        # self.seed = seed
        self.holidays = holidays
        self.feature_list = []
        self._dow_added = False

    def calc_max_window(self):
        max_w = 0
        for _, fd in self.trailing_features_dict.items():
            max_w = max(max_w, max(fd['window']))
        return max_w

    def prep_data(self, df):
        date_cols = ['outDeptDt_dt', 'inDeptDt_dt', 'searchDt_dt']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col])
        return df

    def extract_dow(self, df):
        df['dept_dt_dow'] = df['outDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
        df['return_dt_dow'] = df['inDeptDt_dt'].apply(
                lambda d: datetime.date.weekday(d))
        if not self._dow_added:
            self._dow_added = True
            # self.feature_list.extend(["dept_dt_dow", "return_dt_dow"])
        return df
                       
    def eng_dow_holidays(self, df):

        df = self.extract_dow(df)
        df['dept_dt_is_wkend'] = df['dept_dt_dow'].isin([4,5,6]) # include Friday
        df['return_dt_is_wkend'] = df['return_dt_dow'].isin([5,6]) # exclude Friday

        df['dept_dt_is_hol'] = df['outDeptDt_dt'].isin(self.holidays)
        df['return_dt_is_hol'] = df['inDeptDt_dt'].isin(self.holidays)

        # self.feature_list.extend([
        #     'dept_dt_is_hol', 'dept_dt_is_wkend', 'return_dt_is_hol', 'return_dt_is_wkend'
        #     ])
        
        return df

    # def _generic_shift_feature(self, df, feature_name, kwargs):
    def _generic_shift_feature(self, df, feature_name, sort_col, groupby_cols, shift=1):
        """This takes previous value, irrespective of date.
        If `shift` is not defined in kwargs, it defaults to 1 here.
        """
        df[feature_name] = (df
                        .sort_values(by=sort_col)
                        .groupby(groupby_cols)[self.target_col]
                        .shift(shift)
                    )

        df[feature_name + "_date"] = (df
                .sort_values(by=sort_col)
                .groupby(groupby_cols)
                ['searchDt_dt'].shift(shift) 
            )
        df[feature_name + "_date_diff"] = (
            (df['searchDt_dt'] - df[feature_name + "_date"]) 
            / datetime.timedelta(days=1)
        )
        self.feature_list.append(feature_name)
        return df
    
    def eng_shifted_features(self, df):
        for feature_name, kwargs in self.shifted_features_dict.items():
            self._generic_shift_feature(
                df=df, 
                feature_name=feature_name,
                sort_col=kwargs['sort_col'],
                groupby_cols=kwargs['groupby_cols'],
                shift=kwargs.get("shift", 1)
            )
        return df

    def _generic_trailing_median(self, df, grpby_cols, new_col):
        # NEED TO CHECK THIS
        _grouped = (df
            .groupby(['searchDt_dt'] + grpby_cols)
            .agg({
                self.target_col: [np.median]
            })
            [self.target_col].mean()
           )

        grp_df = pd.DataFrame(_grouped)
        grp_df.columns = ['median']
        grp_df = grp_df.reset_index()
        grp_df = grp_df.set_index(pd.DatetimeIndex(grp_df['searchDt_dt'])).drop(columns=['searchDt_dt'])
        
        window_str = f"{self.trail_avg_window}D"
        _grouped2 = (grp_df.groupby(grpby_cols)
                                .rolling(window_str,
                                    closed='left'
                                )
                                # update to take median of median?
                                # or is mean OK here?
                                ['median'].mean()
                            )
        grp_df2 = pd.DataFrame(_grouped2)
        grp_df2.columns = [new_col]
        grp_df2 = grp_df2.reset_index()

        join_df = df.merge(grp_df2, on=['searchDt_dt'] + grpby_cols, how='left')
        return join_df

    def _generic_trailing_avg(self, df, grpby_cols, feature_name_base, window_list):
        """Trailing average of `grpby_cols` over search dates, returned as series of
        new columns (`feature_name_base`_`window`d).
        """
        _grouped = (df
            .groupby(['searchDt_dt'] + grpby_cols)
            [self.target_col].mean()
           )

        grp_df = pd.DataFrame(_grouped)
        grp_df.columns = ['mean']
        grp_df = grp_df.reset_index()
        grp_df = grp_df.set_index(pd.DatetimeIndex(grp_df['searchDt_dt'])).drop(columns=['searchDt_dt'])
        
        grp_df2 = None
        
        for window in window_list:
            window_str = f"{window}D"
            feature_name = f"{feature_name_base}_{window}d"

            _grouped2 = (grp_df.groupby(grpby_cols)
                                    .rolling(window_str,
                                        closed='left'
                                    )
                                    ['mean'].mean()
                            )
            if grp_df2 is None:
                grp_df2 = pd.DataFrame(_grouped2)
                grp_df2.columns = [feature_name]
                grp_df2 = grp_df2.reset_index()
            else:
                grp_df2[feature_name] = _grouped2.values
            
            self.feature_list.append(feature_name)

        join_df = df.merge(grp_df2, on=['searchDt_dt'] + grpby_cols, how='left')
        return join_df

    def eng_trailing_features(self, df):
        for feature_name_base, kwargs in self.trailing_features_dict.items():
            df = self._generic_trailing_avg(
                df=df, 
                feature_name_base=feature_name_base,
                grpby_cols=kwargs["groupby_cols"],
                window_list=kwargs["window"]
            )
        return df

    def cumulative_avg(self, df, grpby_cols, new_col='cum_avg'):
        # NOTE: not sure we want to use this. But documenting for posterity.
        """Cumulative (i.e. rows unbounded preceeding) average of `grpby_cols` 
        over search dates, returned as `new_col`.
        """
        _grouped = (df
            .groupby(['searchDt_dt', 'shop_ind'] + grpby_cols)
            [self.target_col].mean()
           )

        grp_df = pd.DataFrame(_grouped)
        grp_df.columns = ['mean']
        grp_df = grp_df.reset_index()

        grp_df['cum_sum'] = (grp_df
                            .sort_values(by=['searchDt_dt'])
                            .groupby(grpby_cols)
                            [new_col].cumsum()
        )
        grp_df['cum_count'] = (grp_df
                            .sort_values(by=['searchDt_dt'])
                            .groupby(grpby_cols)
                            ['shop_ind'].cumsum()
        )
        grp_df[new_col] = grp_df['cum_sum'] / grp_df['cum_count']
        join_df = df.merge(grp_df, on=['searchDt_dt'] + grpby_cols, how='left')
        return join_df

    def processs(self, df):
        df = self.prep_data(df)
        eng_df = self.eng_dow_holidays(df)
        eng_df = self.eng_shifted_features(eng_df)
        eng_df = self.eng_trailing_features(eng_df)
        return eng_df
    
