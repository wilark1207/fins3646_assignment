"""
Main module for Project 2

This module includes utilities to run and test the functions in the
`task_project2` module.

IMPORTANT:
Please refer to the project description for further details about this module.
"""
import pandas as pd
import numpy as np

from projects.project2.helpers import (
        fmt_dt,
        fmt_tic,
        read_ma_deals,
        read_org_ff,
        read_stk_rets,
        summarise_series,
        wide_to_long_rets,
        print_msg,
        )

from projects.project2.task_project2 import (
        mk_ma_info,
        mk_stk_arets,
        mean_by_dates,
        expand_event_dates,
        mk_buy_tgt_sell_acq_rets,
        mk_buy_tgt_sell_mkt_rets,
        mk_tgt_rets_by_event_time,
        mk_prop_positive_tgt_rets,
        )


def main():
    """
    Run the full project workflow for analysing the profitability of
    trading strategies around M&A announcements.

    This function performs the following steps:

    1. **Load the input data**
       - Daily stock returns (`ma_rets.csv`)
       - M&A deal information (`ma_deals.csv`)
       - Fama–French factors (`FF_Research_Data_Factors_daily.csv`)

    2. **Construct deal-level information**
       Using `mk_ma_info`, the function organises the deal data into a
       cleaner format with one row per deal and columns identifying the
       acquirer, target, announcement date, and deal number.

    3. **Compute abnormal returns**
       Using `mk_stk_arets`, the function computes abnormal returns for
       each stock and date, defined as the stock return minus the market
       return.

    4. **Expand events across event windows**
       Using `expand_event_dates`, the function creates a table where each
       deal is repeated once for every valid return date from 1 to 30
       days after the announcement.

    5. **Construct trading strategy returns**
       - `mk_buy_tgt_sell_acq_rets` computes daily portfolio returns for a
         strategy that buys the target and sells the acquirer.
       - `mk_buy_tgt_sell_mkt_rets` computes daily portfolio returns for a
         strategy that buys the target and sells the market (using
         abnormal returns).

    6. **Build an event-time panel for target returns**
       Using `mk_tgt_rets_by_event_time`, the function converts target
       returns into a deal-by-event-time matrix.

    7. **Compute event-time positive-return proportions**
       Using `mk_prop_positive_tgt_rets`, the function calculates, for each
       event time, the proportion of deals with a strictly positive target
       return.

    Notes
    -----

    This function will not display any information. See `_test_main` for a
    version of this function which displays information about the
    profitability of trading strategies

    """
    print_msg("Running main...", as_header=True)

    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns 
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
     )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )


    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )

    # 7: Compute event-time positive-return proportions
    prop_positive_tgt_rets = mk_prop_positive_tgt_rets(
        tgt_rets_by_event_time= tgt_rets_by_event_time)


# ----------------------------------------------------------------------------
#  Test functions
#
#  IMPORTANT: If a function is named "test_..." instead of "_test_...",
#  PyCharm will try to debug the function by default. To prevent
#  this behaviour, add a single underscore to the start of test function names.
# ----------------------------------------------------------------------------
def _test_main():
    """
    Copy of the main function that prints information about the profitability
    of trading strategies

    YOU MAY MODIFY THIS FUNCTION IN ANY WAY YOU LIKE.
    """
    print_msg("Running _test_main...", as_header=True)
    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns 
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
    )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )
    print_msg("summarise_series(buy_tgt_sell_acq_rets) ->", 
              summarise_series(buy_tgt_sell_acq_rets), '')

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )
    print_msg("summarise_series(buy_tgt_sell_mkt_rets) ->", 
              summarise_series(buy_tgt_sell_mkt_rets), '')

    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )

    # 7: Compute event-time positive-return proportions
    prop_positive_tgt_rets = mk_prop_positive_tgt_rets(
        tgt_rets_by_event_time= tgt_rets_by_event_time)
    print_msg("summarise_series(prop_positive_tgt_rets- 0.5) ->", 
              summarise_series(prop_positive_tgt_rets - 0.5), '')


#====Ivan's addition===
def _test_mk_ma_info():
    """
    It prints:
      - The first few rows of the raw ma_deals table,
      - The first few rows of mk_ma_info(ma_deals),
      - A comparison of the number of deals vs the number of rows
        in mk_ma_info by announcement date.
    """
    print_msg("Running _test_mk_ma_info...", as_header=True)

    # Read real data
    ma_deals = read_ma_deals()
    ma_info = mk_ma_info(ma_deals)
    ma_deals['announcement'] = pd.to_datetime(ma_deals['announcement'])
    ma_info = mk_ma_info(ma_deals)

    # Show a snapshot of inputs and outputs
    print_msg("ma_deals.head() ->", ma_deals.head(), '')
    print_msg("mk_ma_info(ma_deals).head() ->", ma_info.head(), '')

    # For each announcement date:
    #   n_deals        = number of distinct dealno in the raw data
    #   n_rows_ma_info = number of rows produced by mk_ma_info
    deals_per_date = (
        ma_deals.groupby('announcement')['dealno']
                .nunique()
                .rename('n_deals')
    )

    rows_per_date = (
        ma_info.groupby('announcement')['dealno']
               .size()
               .rename('n_rows_ma_info')
    )

    check = (
        pd.concat([deals_per_date, rows_per_date], axis=1)
          .fillna(0)
          .astype(int)
    )

    print_msg(
        "Deals vs mk_ma_info rows by announcement (first 10 dates) ->",
        check.head(10),
        ''
    )

    # Highlight any dates where mk_ma_info has more rows than deals
    problem_dates = check[check['n_rows_ma_info'] > check['n_deals']]

    if not problem_dates.empty:
        print_msg(
            "Dates with more rows in mk_ma_info than deals ->",
            problem_dates,
            ''
        )

        # Show detailed comparison for the first problematic date
        example_date = problem_dates.index[0]

        print_msg(
            f"Raw ma_deals rows for announcement = {example_date} ->",
            ma_deals[ma_deals['announcement'] == example_date],
            ''
        )

        # ma_info.announcement is datetime64, so convert
        example_dt = pd.to_datetime(example_date)
        print_msg(
            f"mk_ma_info rows for announcement = {example_date} ->",
            ma_info[ma_info['announcement'] == example_dt],
            ''
        )
    else:
        print_msg(
            "No dates where mk_ma_info has more rows than deals "
            "(no duplication visible in this dataset).",
            '',
            ''
        )
def _test_mk_ma_info_has_both_tickers():
    """
    Check that mk_ma_info only includes deals that have BOTH
    an acquirer and a target in the original ma_deals table.
    """
    print_msg("Running _test_mk_ma_info_has_both_tickers...", as_header=True)

    # Read real data and build ma_info
    ma_deals = read_ma_deals()
    ma_info  = mk_ma_info(ma_deals)

    # Deals that have at least one acquirer row
    deals_with_acq = set(
        ma_deals.loc[ma_deals["firmtype"] == "acq", "dealno"]
    )

    # Deals that have at least one target row
    deals_with_tgt = set(
        ma_deals.loc[ma_deals["firmtype"] == "tgt", "dealno"]
    )

    # Deals that have BOTH acquirer and target in the raw data
    deals_with_both = deals_with_acq & deals_with_tgt

    # Deals that appear in mk_ma_info
    deals_in_ma_info = set(ma_info["dealno"])

    # Differences
    extra_in_ma_info = deals_in_ma_info - deals_with_both
    missing_from_ma_info = deals_with_both - deals_in_ma_info

    print_msg("Number of deals with an acquirer row ->",
              len(deals_with_acq), '')
    print_msg("Number of deals with a target row   ->",
              len(deals_with_tgt), '')
    print_msg("Number of deals with BOTH          ->",
              len(deals_with_both), '')
    print_msg("Number of deals in ma_info         ->",
              len(deals_in_ma_info), '')

    print_msg("Deals in ma_info but NOT in 'both' set ->",
              sorted(extra_in_ma_info), '')
    print_msg("Deals with both but missing from ma_info ->",
              sorted(missing_from_ma_info), '')

    # Sanity check: no missing tickers inside ma_info
    print_msg("Any missing acq in ma_info? ->",
              ma_info["acq"].isna().any(), '')
    print_msg("Any missing tgt in ma_info? ->",
              ma_info["tgt"].isna().any(), '')

# Jordan's Changes

def _test_expand_event_dates():
    """
    Copy of the main function that prints information about the profitability
    of trading strategies

    YOU MAY MODIFY THIS FUNCTION IN ANY WAY YOU LIKE.
    """
    print_msg("Running _test_main...", as_header=True)
    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
    )

    print(ma_info)
    print(stk_rets.index)

    print("Expand Dates starts past this \n")
    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )
    

    #Notes:
    
    #  Potential Issue:
    #Seems to select the correct number of days inside the 30day which 
    print(expanded_ma_info.tail(60))
    print(expanded_ma_info.iloc[1582:1601])
    #although the slice might not include the 30th day since end is not included
    # window_idx = dates.loc[start:end].index
    
    #if that's the problem, solution should be to just
    # window_idx = dates.loc[start:end+1].index
    
    
    
    #Has RangeIndex as specified in docstring
    print(type(expanded_ma_info.index))
    
    
    #Datatypes look correct based on doc string in mk_buy_tgt_sell_acq_rets
    expanded_ma_info.info()

    #it printed null with the first test dataframe (event outside of valid date)
    #Aligns with doc string
    import datetime as dt
    test_df = pd.DataFrame({
        'announcement': dt.datetime(year=2020, month=4, day=9),
        'dealno': [47593],
        'acq': ['company2'],
        'tgt': ['company1']
    })
    expanded_ma_info_test = expand_event_dates(
        events=test_df,
        valid_dates=stk_rets.index,
    )
    
    print(expanded_ma_info_test)

    #The below line of code might be slightly redundant, as ignore_index=True already
    #resets the index of the new concat dataframe to integer index but I don't know
    #if that is relevant


    #return pd.concat(out, ignore_index=True).reset_index(drop=True)


def _test_expand_event_dates1():
    print("Running _test_expand_event_dates...")
    """
        Function operated as intended

        Rationale:
            expand_event_dates(events, valid_dates, ...) builds an expanded
            event-level table by:
              - Taking each row in `events` (one row per deal),
              - For each event, generating one row for every date in
                `valid_dates` that lies between 1 and 30 calendar days
                after the announcement date (inclusive),
              - Returning a DataFrame that contains the original event
                columns plus a `date` column, with a RangeIndex.

            Internally, the function collects all expanded rows into a list
            `out`. If at least one event has valid dates in the 1–30 day
            window, `out` is non-empty and the function returns a
            concatenated DataFrame. If no event has any matching dates
            in `valid_dates`, `out` remains empty, the `if out:` block
            is skipped, and the function implicitly returns None.

            This behaviour is consistent with the docstring:
                "If no event has any matching dates in valid_dates,
                 returns None."

        Real-data behaviour:
            Using the project data, we check that:
              1) The output is a non-empty DataFrame.
              2) The index is a RangeIndex.
              3) The `date` and `announcement` columns are datetime64[ns].
              4) The `dealno` column is integer dtype.
              5) `acq` and `tgt` are object (string) dtypes.
              6) All `date` values lie in `valid_dates`.
              7) event_time = (date - announcement).days is between
                 1 and 30 inclusive.

        Synthetic example (single event, clean daily dates):
            events_ex = pd.DataFrame({
                'announcement': [pd.Timestamp(2021, 1, 4)],
                'dealno': [1],
                'acq': ['AAA'],
                'tgt': ['BBB'],
            })
            valid_dates_ex = pd.date_range('2021-01-01', '2021-02-10', freq='D')

            For this setup, the expanded table should contain one row for
            each date from 2021-01-05 to 2021-02-03 inclusive (1 to 30
            calendar days after the announcement), with the original event
            fields (announcement, dealno, acq, tgt) repeated.

        Edge case 1 (no overlap, must return None):
            If an event's announcement date is so far from `valid_dates`
            that the entire 1–30 day window has no overlap, the function
            should return None.

            Example:
                events_edge = pd.DataFrame({
                    'announcement': [pd.Timestamp(2020, 4, 9)],
                    'dealno': [99999],
                    'acq': ['COMP2'],
                    'tgt': ['COMP1'],
                })

                valid_dates_edge = stk_rets.index  # 2021 trading days

                expand_event_dates(events_edge, valid_dates_edge)
                -> None

        Edge case 2 (unsorted and duplicated valid_dates):
            We construct valid_dates with duplicates and out-of-order dates
            to confirm that:
              - The function drops duplicates,
              - Sorts valid_dates,
              - And produces unique (dealno, date) combinations.

        Edge case 3 (multiple events with different announcement dates):
            We test two events with different announcement dates and a
            common valid_dates range, and check that each deal has its
            own correct event_time window (1–30 days) without mixing rows
            across deals.
    """

    # ----------------------------------------------------------
    # 1) REAL DATA CHECKS
    # ----------------------------------------------------------
    stk_rets = read_stk_rets()  # DatetimeIndex of trading days
    ma_deals = read_ma_deals()
    ma_info = mk_ma_info(ma_deals)  # one row per deal, with announcement/acq/tgt

    expanded = expand_event_dates(
        events=ma_info,
        valid_dates=stk_rets.index,
    )

    # ---------- TYPE CHECKS ----------
    if expanded is None:
        raise Exception("expand_event_dates returned None for real data, expected a DataFrame")
    if not isinstance(expanded, pd.DataFrame):
        raise Exception(f"expand_event_dates did not return a DataFrame, got {type(expanded)}")

    # Index type: should be a RangeIndex (as per docstring)
    if not isinstance(expanded.index, pd.RangeIndex):
        raise Exception(f"Expanded DataFrame index is not a RangeIndex, got {type(expanded.index)}")

    # Required columns present
    required_cols = {'date', 'announcement', 'dealno', 'acq', 'tgt'}
    missing_cols = required_cols - set(expanded.columns)
    if missing_cols:
        raise Exception(f"Expanded DataFrame is missing required columns: {missing_cols}")

    # Dtype checks for core columns
    if not pd.api.types.is_datetime64_any_dtype(expanded['date']):
        raise Exception("Column 'date' is not datetime64[ns]")
    if not pd.api.types.is_datetime64_any_dtype(expanded['announcement']):
        raise Exception("Column 'announcement' is not datetime64[ns]")
    if not pd.api.types.is_integer_dtype(expanded['dealno']):
        raise Exception("Column 'dealno' is not an integer dtype")
    if not pd.api.types.is_object_dtype(expanded['acq']):
        raise Exception("Column 'acq' is not object (string) dtype")
    if not pd.api.types.is_object_dtype(expanded['tgt']):
        raise Exception("Column 'tgt' is not object (string) dtype")

    # ---------- CONTENT / LOGIC CHECKS ----------
    # All expanded dates must be in valid_dates
    valid_dates_set = set(stk_rets.index)
    expanded_dates_set = set(expanded['date'])
    if not expanded_dates_set.issubset(valid_dates_set):
        bad_dates = expanded_dates_set - valid_dates_set
        raise Exception(f"Expanded dates include values not in valid_dates: {sorted(bad_dates)[:10]}")

    # Check event_time between 1 and 30 inclusive
    tmp = expanded[['date', 'announcement']].copy()
    tmp['event_time'] = (tmp['date'] - tmp['announcement']).dt.days
    et_min = tmp['event_time'].min()
    et_max = tmp['event_time'].max()

    if et_min < 1 or et_max > 30:
        raise Exception(
            f"event_time is out of expected range [1, 30]. "
            f"Got min={et_min}, max={et_max}"
        )

    # ----------------------------------------------------------
    # 2) SYNTHETIC EXAMPLE (SINGLE EVENT, DAILY DATES)
    # ----------------------------------------------------------
    events_ex = pd.DataFrame({
        'announcement': [pd.Timestamp(2021, 1, 4)],
        'dealno': [1],
        'acq': ['AAA'],
        'tgt': ['BBB'],
    })
    valid_dates_ex = pd.date_range('2021-01-01', '2021-02-10', freq='D')

    expanded_ex = expand_event_dates(events_ex, valid_dates_ex)

    if expanded_ex is None:
        raise Exception("Synthetic example returned None, expected a non-empty DataFrame")
    if not isinstance(expanded_ex, pd.DataFrame):
        raise Exception(f"Synthetic example: expected DataFrame, got {type(expanded_ex)}")

    # Expected dates: 1 to 30 days after 2021-01-04 inclusive
    expected_dates_ex = pd.date_range('2021-01-05', '2021-02-03', freq='D')
    got_dates_ex = expanded_ex['date'].sort_values().reset_index(drop=True)

    if not got_dates_ex.equals(expected_dates_ex.to_series().reset_index(drop=True)):
        raise Exception(
            "Synthetic example: expanded dates do not match expected 1–30 day window.\n"
            f"Expected: {list(expected_dates_ex)}\n"
            f"Got:      {list(got_dates_ex)}"
        )

    # Core columns preserved & constant
    uniq_ann = expanded_ex['announcement'].unique()
    uniq_deal = expanded_ex['dealno'].unique()
    uniq_acq = expanded_ex['acq'].unique()
    uniq_tgt = expanded_ex['tgt'].unique()

    if len(uniq_ann) != 1 or uniq_ann[0] != pd.Timestamp(2021, 1, 4):
        raise Exception(f"Synthetic example: announcement not preserved correctly: {uniq_ann}")
    if len(uniq_deal) != 1 or uniq_deal[0] != 1:
        raise Exception(f"Synthetic example: dealno not preserved correctly: {uniq_deal}")
    if len(uniq_acq) != 1 or uniq_acq[0] != 'AAA':
        raise Exception(f"Synthetic example: acq not preserved correctly: {uniq_acq}")
    if len(uniq_tgt) != 1 or uniq_tgt[0] != 'BBB':
        raise Exception(f"Synthetic example: tgt not preserved correctly: {uniq_tgt}")

    # ----------------------------------------------------------
    # 3) EDGE CASE 1: NO OVERLAP WITH valid_dates -> MUST RETURN None
    # ----------------------------------------------------------
    events_edge = pd.DataFrame({
        'announcement': [pd.Timestamp(2020, 4, 9)],
        'dealno': [99999],
        'acq': ['COMP2'],
        'tgt': ['COMP1'],
    })
    expanded_edge = expand_event_dates(events_edge, stk_rets.index)

    if expanded_edge is not None:
        raise Exception(
            "Expected expand_event_dates to return None when no event "
            "has matching dates in valid_dates, but got a DataFrame instead."
        )

    # ----------------------------------------------------------
    # 4) EDGE CASE 2: UNSORTED + DUPLICATED valid_dates
    # ----------------------------------------------------------
    # Build valid_dates with duplicates and reversed order
    vd_raw = pd.to_datetime([
        "2021-01-05", "2021-01-06", "2021-01-05", "2021-01-07", "2021-01-06"
    ])
    valid_dates_dup = pd.DatetimeIndex(vd_raw[::-1])  # reverse order

    events_dup = pd.DataFrame({
        'announcement': [pd.Timestamp(2021, 1, 4)],
        'dealno': [10],
        'acq': ['ACQ10'],
        'tgt': ['TGT10'],
    })

    expanded_dup = expand_event_dates(events_dup, valid_dates_dup)

    if expanded_dup is None:
        raise Exception("Duplicate/unsorted valid_dates test returned None, expected DataFrame")

    # Expect unique dates 2021-01-05, 06, 07 (1–3 days after announcement)
    expected_dates_dup = pd.to_datetime(["2021-01-05", "2021-01-06", "2021-01-07"])
    got_dates_dup = expanded_dup['date'].sort_values().unique()

    if len(got_dates_dup) != len(expected_dates_dup) or not (got_dates_dup == expected_dates_dup).all():
        raise Exception(
            "Duplicate/unsorted valid_dates handling failed.\n"
            f"Expected dates: {list(expected_dates_dup)}\n"
            f"Got dates:      {list(got_dates_dup)}"
        )

    # Also ensure there is exactly one row per date (no duplicated (dealno, date))
    if expanded_dup.duplicated(subset=['dealno', 'date']).any():
        raise Exception("Duplicate rows found for the same (dealno, date) in expanded_dup")

    # ----------------------------------------------------------
    # 5) EDGE CASE 3: MULTIPLE EVENTS WITH DIFFERENT ANNOUNCEMENT DATES
    # ----------------------------------------------------------
    events_multi = pd.DataFrame({
        'announcement': [pd.Timestamp(2021, 1, 4), pd.Timestamp(2021, 1, 10)],
        'dealno': [100, 200],
        'acq': ['ACQ100', 'ACQ200'],
        'tgt': ['TGT100', 'TGT200'],
    })
    valid_dates_multi = pd.date_range('2021-01-01', '2021-02-20', freq='D')

    expanded_multi = expand_event_dates(events_multi, valid_dates_multi)
    if expanded_multi is None:
        raise Exception("Multiple-events test returned None, expected a DataFrame")

    # Compute event_time again and check ranges per deal
    multi_tmp = expanded_multi.copy()
    multi_tmp['event_time'] = (multi_tmp['date'] - multi_tmp['announcement']).dt.days

    for dealno in [100, 200]:
        deal_rows = multi_tmp[multi_tmp['dealno'] == dealno]
        if deal_rows.empty:
            raise Exception(f"Multiple-events test: no rows produced for deal {dealno}")
        et_min_d = deal_rows['event_time'].min()
        et_max_d = deal_rows['event_time'].max()
        if et_min_d < 1 or et_max_d > 30:
            raise Exception(
                f"Multiple-events test: event_time out of [1, 30] for deal {dealno}. "
                f"Got min={et_min_d}, max={et_max_d}"
            )

    print("_test_expand_event_dates passed successfully!")

def _test_mk_buy_tgt_sell_acq_rets():
    print("Running _test_mk_buy_tgt_sell_acq_rets...")
    """
        Rationale:
            mk_buy_tgt_sell_acq_rets(expanded_ma_info, stk_rets) computes,
            for each date:
                mean(target returns) - mean(acquirer returns)
            across all deals active on that date, with equal weights
            across deals.

        Important implementation detail:
            Inside the loop over dates, we must treat each date-slice of
            `events` and `rets` as a DataFrame, even if there is only ONE
            event/row for that date. This requires using:

                rets_date   = rets.loc[[date]].set_index(['date', 'ticker'])
                events_date = events.loc[[date]]

            instead of `.loc[date]`, which can return a Series and break
            `.rename(columns=...).set_index(...)`.

        This test checks:

          1) Real-data behaviour (smoke test):
             - Returns a non-empty pandas Series.
             - Index is a DatetimeIndex, sorted ascending.
             - Dtype is numeric (float-like).
             - All dates lie inside stk_rets.index.
             - No NaN after dropna().

          2) Synthetic example 1 (SINGLE deal, TWO dates):
             - One deal, one acquirer, one target.
             - This case would previously have failed when using `.loc[date]`
               on `events`.
             - For each date, we verify:
                   result = ret_tgt(date) - ret_acq(date)

          3) Synthetic example 2 (TWO deals on SAME date):
             - Two deals active on a single date.
             - We verify:
                   result = mean(target returns) - mean(acquirer returns)

          4) Synthetic example 3 (DATE with NO overlap in stk_rets):
             - expanded_ma_info has two dates, but stk_rets only has one.
             - We verify that ONLY the overlapping date appears in the result.
    """

    # ----------------------------------------------------------
    # 1) REAL DATA SMOKE TEST
    # ----------------------------------------------------------
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    ma_info = mk_ma_info(ma_deals)
    expanded_ma_info = expand_event_dates(
        events=ma_info,
        valid_dates=stk_rets.index,
    )

    strat_rets = mk_buy_tgt_sell_acq_rets(
        expanded_ma_info=expanded_ma_info,
        stk_rets=stk_rets,
    )

    # ---- Type & structure checks ----
    if not isinstance(strat_rets, pd.Series):
        raise Exception(
            f"mk_buy_tgt_sell_acq_rets did not return a Series, "
            f"got {type(strat_rets)}"
        )

    if strat_rets.empty:
        raise Exception("Strategy returns Series is empty on real data (unexpected)")

    if not isinstance(strat_rets.index, pd.DatetimeIndex):
        raise Exception("Strategy returns index is not a DatetimeIndex")

    if not strat_rets.index.is_monotonic_increasing:
        raise Exception("Strategy return index is not sorted in ascending order")

    # dtype: allow any numeric (covers float64, possibly nullable float)
    if not pd.api.types.is_numeric_dtype(strat_rets.dtype):
        raise Exception(f"Strategy return dtype is not numeric: {strat_rets.dtype}")

    if strat_rets.isna().any():
        raise Exception("Strategy returns contain NaN values after dropna()")

    if not set(strat_rets.index).issubset(set(stk_rets.index)):
        bad_dates = set(strat_rets.index) - set(stk_rets.index)
        raise Exception(
            "Strategy returns include dates not present in stk_rets.index. "
            f"Examples: {sorted(bad_dates)[:10]}"
        )

    # ----------------------------------------------------------
    # 2) SYNTHETIC EXAMPLE 1: SINGLE DEAL, TWO DATES
    #    → This enforces correct use of .loc[[date]] (DataFrame, not Series)
    # ----------------------------------------------------------
    dates_small = pd.to_datetime(["2021-01-02", "2021-01-03"])

    # One acquirer (ACQ1) and one target (TGT1)
    stk_small = pd.DataFrame(
        {
            "ACQ1": [0.10, 0.00],
            "TGT1": [0.20, 0.30],
        },
        index=dates_small,
    )

    expanded_small = pd.DataFrame(
        {
            "date": dates_small,
            "announcement": [pd.Timestamp("2021-01-01")] * 2,
            "dealno": [1, 1],
            "acq": ["ACQ1", "ACQ1"],
            "tgt": ["TGT1", "TGT1"],
        }
    )

    got_small = mk_buy_tgt_sell_acq_rets(
        expanded_ma_info=expanded_small,
        stk_rets=stk_small,
    )

    expected_small = pd.Series(
        data=[0.20 - 0.10, 0.30 - 0.00],
        index=dates_small,
    ).sort_index()

    if not got_small.index.equals(expected_small.index):
        raise Exception(
            "Synthetic example 1 index mismatch.\n"
            f"Expected index: {list(expected_small.index)}\n"
            f"Got index:      {list(got_small.index)}"
        )

    if not np.allclose(got_small.values, expected_small.values):
        raise Exception(
            "Synthetic example 1 value mismatch.\n"
            f"Expected: {list(expected_small.values)}\n"
            f"Got:      {list(got_small.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_small.dtype):
        raise Exception(f"Synthetic example 1: dtype is not numeric: {got_small.dtype}")

    # ----------------------------------------------------------
    # 3) SYNTHETIC EXAMPLE 2: TWO DEALS ON SAME DATE (EQUAL WEIGHTS)
    # ----------------------------------------------------------
    date_one = pd.to_datetime(["2021-02-01"])

    stk_multi = pd.DataFrame(
        {
            "ACQ1": [0.05],
            "ACQ2": [0.15],
            "TGT1": [0.10],
            "TGT2": [0.20],
        },
        index=date_one,
    )

    expanded_multi = pd.DataFrame(
        {
            "date": [date_one[0], date_one[0]],
            "announcement": [pd.Timestamp("2021-01-25"), pd.Timestamp("2021-01-25")],
            "dealno": [1, 2],
            "acq": ["ACQ1", "ACQ2"],
            "tgt": ["TGT1", "TGT2"],
        }
    )

    got_multi = mk_buy_tgt_sell_acq_rets(
        expanded_ma_info=expanded_multi,
        stk_rets=stk_multi,
    )

    # Expected:
    #   mean_tgt = (0.10 + 0.20) / 2 = 0.15
    #   mean_acq = (0.05 + 0.15) / 2 = 0.10
    #   result   = 0.15 - 0.10 = 0.05
    expected_multi = pd.Series(
        data=[0.05],
        index=date_one,
    )

    if not got_multi.index.equals(expected_multi.index):
        raise Exception(
            "Synthetic example 2 index mismatch.\n"
            f"Expected index: {list(expected_multi.index)}\n"
            f"Got index:      {list(got_multi.index)}"
        )

    if not np.allclose(got_multi.values, expected_multi.values):
        raise Exception(
            "Synthetic example 2 value mismatch.\n"
            f"Expected: {list(expected_multi.values)}\n"
            f"Got:      {list(got_multi.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_multi.dtype):
        raise Exception(f"Synthetic example 2: dtype is not numeric: {got_multi.dtype}")

    # ----------------------------------------------------------
    # 4) SYNTHETIC EXAMPLE 3: DATE WITH NO OVERLAP IN stk_rets
    # ----------------------------------------------------------
    dates_part = pd.to_datetime(["2021-03-01", "2021-03-02"])

    # stk_rets only for 2021-03-01
    stk_part = pd.DataFrame(
        {
            "ACQX": [0.10],
            "TGTX": [0.20],
        },
        index=dates_part[:1],
    )

    expanded_part = pd.DataFrame(
        {
            "date": dates_part,   # two dates: 1st and 2nd
            "announcement": [pd.Timestamp("2021-02-25")] * 2,
            "dealno": [10, 10],
            "acq": ["ACQX", "ACQX"],
            "tgt": ["TGTX", "TGTX"],
        }
    )

    got_part = mk_buy_tgt_sell_acq_rets(
        expanded_ma_info=expanded_part,
        stk_rets=stk_part,
    )

    expected_index_part = dates_part[:1]  # only the overlapping date

    if not got_part.index.equals(expected_index_part):
        raise Exception(
            "Synthetic example 3: output index does not match expected overlapping date.\n"
            f"Expected index: {list(expected_index_part)}\n"
            f"Got index:      {list(got_part.index)}"
        )

    if not pd.api.types.is_numeric_dtype(got_part.dtype):
        raise Exception(f"Synthetic example 3: dtype is not numeric: {got_part.dtype}")

    print("✅ _test_mk_buy_tgt_sell_acq_rets passed successfully!")

def _test_mk_buy_tgt_sell_mkt_rets():
    print("Running _test_mk_buy_tgt_sell_mkt_rets...")
    """
    Tests for mk_buy_tgt_sell_mkt_rets(expanded_ma_info, stk_arets)

    Behaviour being verified
    ------------------------
    For each date:
      1. Join expanded_ma_info's target tickers (tgt) with stk_arets on (date, ticker).
      2. For each (date, deal), the strategy return = target abnormal return (aret).
      3. Portfolio return on that date = equal-weighted mean of aret across all active deals.
      4. Output = pandas Series, indexed by date (DatetimeIndex), sorted ascending, numeric.

    This test covers:

      A) Real-data smoke test:
         - Non-empty Series.
         - DatetimeIndex, sorted ascending.
         - Numeric dtype, no NaNs after dropna().
         - Output dates ⊆ stk_arets['date'].

      B) Synthetic Example 1: SINGLE deal, MULTIPLE dates
         - One deal, one target, known arets.
         - Output equals that target's aret series.

      C) Synthetic Example 2: MULTIPLE deals on SAME date
         - Two deals, two different targets on one date.
         - Output equals simple average: mean(aret_T1, aret_T2).

      D) Synthetic Example 3: DATE with NO OVERLAP in stk_arets
         - expanded_ma_info has two dates, stk_arets only one.
         - Output only contains the overlapping date.

      E) Synthetic Example 4: SOME targets missing arets
         - Two deals in expanded_ma_info, but stk_arets only has aret for one target.
         - Output uses only the available target (inner join behaviour).

      F) Synthetic Example 5: DUPLICATE rows in expanded_ma_info
         - Same (date, tgt) appears multiple times.
         - Output counts duplicates separately (as multiple equal-weight positions),
           so the expected value is mean over all rows after the join.

      G) Synthetic Example 6: EMPTY expanded_ma_info
         - expanded_ma_info with correct columns but no rows.
         - Output should be an empty Series (len == 0).
    """

    # ----------------------------------------------------------
    # A) REAL DATA SMOKE TEST
    # ----------------------------------------------------------
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    ma_info = mk_ma_info(ma_deals)
    org_ff = read_org_ff()

    stk_arets = mk_stk_arets(
        stk_rets=stk_rets,
        org_ff=org_ff,
    )

    expanded_ma_info = expand_event_dates(
        events=ma_info,
        valid_dates=stk_rets.index,
    )

    strat_rets = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_ma_info,
        stk_arets=stk_arets,
    )

    # ---- Type & structure checks ----
    if not isinstance(strat_rets, pd.Series):
        raise Exception(
            f"mk_buy_tgt_sell_mkt_rets did not return a Series, "
            f"got {type(strat_rets)}"
        )

    if strat_rets.empty:
        raise Exception("Strategy returns Series is empty on real data (unexpected)")

    if not isinstance(strat_rets.index, pd.DatetimeIndex):
        raise Exception("Strategy returns index is not a DatetimeIndex")

    if not strat_rets.index.is_monotonic_increasing:
        raise Exception("Strategy return index is not sorted in ascending order")

    if not pd.api.types.is_numeric_dtype(strat_rets.dtype):
        raise Exception(f"Strategy return dtype is not numeric: {strat_rets.dtype}")

    if strat_rets.isna().any():
        raise Exception("Strategy returns contain NaN values after dropna()")

    valid_dates = set(stk_arets['date'])
    if not set(strat_rets.index).issubset(valid_dates):
        bad_dates = set(strat_rets.index) - valid_dates
        raise Exception(
            "Strategy returns include dates not present in stk_arets['date']. "
            f"Examples: {sorted(bad_dates)[:10]}"
        )

    # ----------------------------------------------------------
    # B) Synthetic Example 1: SINGLE deal, MULTIPLE dates
    # ----------------------------------------------------------
    dates_small = pd.to_datetime(["2021-01-02", "2021-01-03"])

    stk_arets_small = pd.DataFrame(
        {
            "date": [dates_small[0], dates_small[1]],
            "ticker": ["TGT1", "TGT1"],
            "aret": [0.05, 0.10],
        }
    )

    expanded_small = pd.DataFrame(
        {
            "date": dates_small,
            "announcement": [pd.Timestamp("2021-01-01")] * 2,
            "dealno": [1, 1],
            "acq": ["ACQ1", "ACQ1"],
            "tgt": ["TGT1", "TGT1"],
        }
    )

    got_small = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_small,
        stk_arets=stk_arets_small,
    )

    expected_small = pd.Series(
        data=[0.05, 0.10],
        index=dates_small,
    ).sort_index()

    if not got_small.index.equals(expected_small.index):
        raise Exception(
            "Synthetic example 1 index mismatch.\n"
            f"Expected index: {list(expected_small.index)}\n"
            f"Got index:      {list(got_small.index)}"
        )

    if not np.allclose(got_small.values, expected_small.values):
        raise Exception(
            "Synthetic example 1 value mismatch.\n"
            f"Expected: {list(expected_small.values)}\n"
            f"Got:      {list(got_small.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_small.dtype):
        raise Exception(f"Synthetic example 1: dtype is not numeric: {got_small.dtype}")

    # ----------------------------------------------------------
    # C) Synthetic Example 2: MULTIPLE deals on SAME date
    # ----------------------------------------------------------
    date_one = pd.to_datetime(["2021-02-01"])

    stk_arets_multi = pd.DataFrame(
        {
            "date": [date_one[0], date_one[0]],
            "ticker": ["TGT1", "TGT2"],
            "aret": [0.04, 0.12],
        }
    )

    expanded_multi = pd.DataFrame(
        {
            "date": [date_one[0], date_one[0]],
            "announcement": [pd.Timestamp("2021-01-25"), pd.Timestamp("2021-01-25")],
            "dealno": [1, 2],
            "acq": ["ACQ1", "ACQ2"],
            "tgt": ["TGT1", "TGT2"],
        }
    )

    got_multi = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_multi,
        stk_arets=stk_arets_multi,
    )

    expected_multi = pd.Series(
        data=[(0.04 + 0.12) / 2],
        index=date_one,
    )

    if not got_multi.index.equals(expected_multi.index):
        raise Exception(
            "Synthetic example 2 index mismatch.\n"
            f"Expected index: {list(expected_multi.index)}\n"
            f"Got index:      {list(got_multi.index)}"
        )

    if not np.allclose(got_multi.values, expected_multi.values):
        raise Exception(
            "Synthetic example 2 value mismatch.\n"
            f"Expected: {list(expected_multi.values)}\n"
            f"Got:      {list(got_multi.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_multi.dtype):
        raise Exception(f"Synthetic example 2: dtype is not numeric: {got_multi.dtype}")

    # ----------------------------------------------------------
    # D) Synthetic Example 3: DATE with NO OVERLAP in stk_arets
    # ----------------------------------------------------------
    dates_part = pd.to_datetime(["2021-03-01", "2021-03-02"])

    stk_arets_part = pd.DataFrame(
        {
            "date": [dates_part[0]],   # only first date has aret
            "ticker": ["TGTX"],
            "aret": [0.07],
        }
    )

    expanded_part = pd.DataFrame(
        {
            "date": dates_part,   # two dates
            "announcement": [pd.Timestamp("2021-02-25")] * 2,
            "dealno": [10, 10],
            "acq": ["ACQX", "ACQX"],
            "tgt": ["TGTX", "TGTX"],
        }
    )

    got_part = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_part,
        stk_arets=stk_arets_part,
    )

    expected_index_part = dates_part[:1]  # only overlapping date

    if not got_part.index.equals(expected_index_part):
        raise Exception(
            "Synthetic example 3: output index does not match expected overlapping date.\n"
            f"Expected index: {list(expected_index_part)}\n"
            f"Got index:      {list(got_part.index)}"
        )

    if not pd.api.types.is_numeric_dtype(got_part.dtype):
        raise Exception(f"Synthetic example 3: dtype is not numeric: {got_part.dtype}")

    # ----------------------------------------------------------
    # E) Synthetic Example 4: SOME targets missing arets
    # ----------------------------------------------------------
    date_mix = pd.to_datetime(["2021-04-01"])

    stk_arets_mix = pd.DataFrame(
        {
            "date": [date_mix[0]],
            "ticker": ["TGT_KEEP"],   # only one target has aret
            "aret": [0.09],
        }
    )

    expanded_mix = pd.DataFrame(
        {
            "date": [date_mix[0], date_mix[0]],
            "announcement": [pd.Timestamp("2021-03-20"), pd.Timestamp("2021-03-20")],
            "dealno": [100, 101],
            "acq": ["ACQ_A", "ACQ_B"],
            "tgt": ["TGT_KEEP", "TGT_MISSING"],  # second target has no aret
        }
    )

    got_mix = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_mix,
        stk_arets=stk_arets_mix,
    )

    expected_mix = pd.Series(
        data=[0.09],   # only TGT_KEEP contributes
        index=date_mix,
    )

    if not got_mix.index.equals(expected_mix.index):
        raise Exception(
            "Synthetic example 4 index mismatch.\n"
            f"Expected index: {list(expected_mix.index)}\n"
            f"Got index:      {list(got_mix.index)}"
        )

    if not np.allclose(got_mix.values, expected_mix.values):
        raise Exception(
            "Synthetic example 4 value mismatch.\n"
            f"Expected: {list(expected_mix.values)}\n"
            f"Got:      {list(got_mix.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_mix.dtype):
        raise Exception(f"Synthetic example 4: dtype is not numeric: {got_mix.dtype}")

    # ----------------------------------------------------------
    # F) Synthetic Example 5: DUPLICATE rows in expanded_ma_info
    # ----------------------------------------------------------
    # Same (date, tgt) appears twice; aret counted twice in mean
    date_dup = pd.to_datetime(["2021-05-01"])

    stk_arets_dup = pd.DataFrame(
        {
            "date": [date_dup[0], date_dup[0]],
            "ticker": ["T1", "T2"],
            "aret": [0.10, 0.30],
        }
    )

    expanded_dup = pd.DataFrame(
        {
            "date": [date_dup[0], date_dup[0], date_dup[0]],
            "announcement": [
                pd.Timestamp("2021-04-20"),
                pd.Timestamp("2021-04-20"),
                pd.Timestamp("2021-04-20"),
            ],
            "dealno": [200, 201, 202],
            "acq": ["A1", "A2", "A3"],
            # T1 appears twice, T2 once
            "tgt": ["T1", "T1", "T2"],
        }
    )

    got_dup = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=expanded_dup,
        stk_arets=stk_arets_dup,
    )

    # Join result has arets [0.10, 0.10, 0.30] → mean = (0.10 + 0.10 + 0.30) / 3 = 0.166666...
    expected_dup = pd.Series(
        data=[(0.10 + 0.10 + 0.30) / 3],
        index=date_dup,
    )

    if not got_dup.index.equals(expected_dup.index):
        raise Exception(
            "Synthetic example 5 index mismatch.\n"
            f"Expected index: {list(expected_dup.index)}\n"
            f"Got index:      {list(got_dup.index)}"
        )

    if not np.allclose(got_dup.values, expected_dup.values):
        raise Exception(
            "Synthetic example 5 value mismatch.\n"
            f"Expected: {list(expected_dup.values)}\n"
            f"Got:      {list(got_dup.values)}"
        )

    if not pd.api.types.is_numeric_dtype(got_dup.dtype):
        raise Exception(f"Synthetic example 5: dtype is not numeric: {got_dup.dtype}")

    # ----------------------------------------------------------
    # G) Synthetic Example 6: EMPTY expanded_ma_info
    # ----------------------------------------------------------
    empty_expanded = pd.DataFrame(
        {
            "date": pd.to_datetime([]),
            "announcement": pd.to_datetime([]),
            "dealno": pd.Series([], dtype="int64"),
            "acq": pd.Series([], dtype="object"),
            "tgt": pd.Series([], dtype="object"),
        }
    )

    # Even with a non-empty stk_arets, empty events → empty output
    stk_arets_nonempty = pd.DataFrame(
        {
            "date": [pd.Timestamp("2021-06-01")],
            "ticker": ["TGTZ"],
            "aret": [0.15],
        }
    )

    got_empty = mk_buy_tgt_sell_mkt_rets(
        expanded_ma_info=empty_expanded,
        stk_arets=stk_arets_nonempty,
    )

    if not isinstance(got_empty, pd.Series):
        raise Exception(
            "Synthetic example 6: expected a Series for empty expanded_ma_info"
        )

    if len(got_empty) != 0:
        raise Exception(
            f"Synthetic example 6: expected empty Series, got length {len(got_empty)}"
        )

    print("_test_mk_buy_tgt_sell_mkt_rets passed successfully!")




# Jordan more test functions

def _test_mk_tgt_rets_by_event_time():
    """
    Copy of the main function that prints information about the profitability
    of trading strategies

    YOU MAY MODIFY THIS FUNCTION IN ANY WAY YOU LIKE.
    """
    print_msg("Running _test_main...", as_header=True)
    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
    )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )
    print_msg("summarise_series(buy_tgt_sell_acq_rets) ->",
              summarise_series(buy_tgt_sell_acq_rets), '')

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )
    print_msg("summarise_series(buy_tgt_sell_mkt_rets) ->",
              summarise_series(buy_tgt_sell_mkt_rets), '')

    print(stk_rets)
    stk_rets.info()
    print(expanded_ma_info.head(50))
    expanded_ma_info.info()
    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )
    print(tgt_rets_by_event_time)
    tgt_rets_by_event_time.info()
    print(tgt_rets_by_event_time.head(60))

    import datetime as dt

    print('\n expanded_ma_info')
    print(expanded_ma_info)
    print('\n stk_rets')
    print(stk_rets)
    print('\n tgt_rets_by_event_time')
    print(tgt_rets_by_event_time)

    tgt_rets_by_event_time.info()





    print('\n expanded_ma_info')
    print(expanded_ma_info[expanded_ma_info['dealno'] == 2647141020])
    print('\n stk_rets')
    print(stk_rets.loc[dt.datetime(2021,4,12):dt.datetime(2021,5,13),'NUAN'])
    print('\n tgt_rets_by_event_time')
    print(tgt_rets_by_event_time.loc[:,2647141020])



def _test_mk_prop_positive_tgt_rets():
    """
    Copy of the main function that prints information about the profitability
    of trading strategies

    YOU MAY MODIFY THIS FUNCTION IN ANY WAY YOU LIKE.
    """
    print_msg("Running _test_main...", as_header=True)
    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
    )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )
    print_msg("summarise_series(buy_tgt_sell_acq_rets) ->",
              summarise_series(buy_tgt_sell_acq_rets), '')

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )
    print_msg("summarise_series(buy_tgt_sell_mkt_rets) ->",
              summarise_series(buy_tgt_sell_mkt_rets), '')

    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )

    # 7: Compute event-time positive-return proportions
    prop_positive_tgt_rets = mk_prop_positive_tgt_rets(
        tgt_rets_by_event_time= tgt_rets_by_event_time)
    #print_msg("summarise_series(prop_positive_tgt_rets- 0.5) ->",
              #summarise_series(prop_positive_tgt_rets - 0.5), '')
    #print(tgt_rets_by_event_time)
    #print(prop_positive_tgt_rets)


    #7 operates as intended since NaN>0 evaluates to false
    #list = [True,True,False]
    #list_2 = [False,False,False]
    #list_3 = [True, False, False]
    #test_df = pd.DataFrame(data = {'list_1':list,'list_2':list_2,'list_3':list_3})
    #print(test_df)
    #print(test_df.mean(axis = 1))

    print("old version")
    old_version = (tgt_rets_by_event_time > 0).mean(axis=1)
    print(old_version)

    print("new version")
    print(prop_positive_tgt_rets)

    print(prop_positive_tgt_rets >= old_version)

    print(tgt_rets_by_event_time)
    print(tgt_rets_by_event_time.iloc[17:21,0:4])

    print((tgt_rets_by_event_time.iloc[17:21,0:4] > 0).mean(axis=1))

    m = mk_prop_positive_tgt_rets(tgt_rets_by_event_time.iloc[17:21,0:4])
    print(m)
    #Originally this
    #return (tgt_rets_by_event_time > 0).mean(axis=1)

    #this fails to ingore Nan (which automatically get evaluated as false

    #numerators = (tgt_rets_by_event_time > 0 & type(tgt_rets_by_event_time) == int).sum(axis=1)
    #denominators = (type(tgt_rets_by_event_time) == int).sum(axis=1)
    #sample_proportions = numerators / denominators
    #return sample_proportions




# ----------------------------------------------------------------------------
#  Function to run all other tests
# ----------------------------------------------------------------------------
def run_tests():
    """
    Run all test functions.

    You may complete or extend this function in any way you like.
    """
    print("Running tests...\n")

    # Uncomment to run
    _test_main()

    # Add other function calls here
    #Ivan's addition, uncomment if needed.
    #_test_mk_ma_info() 
    #_test_mk_ma_info_has_both_tickers()
    # _test_expand_event_dates1()
    # _test_mk_buy_tgt_sell_acq_rets()
    # _test_mk_buy_tgt_sell_mkt_rets()
    #_test_mk_tgt_rets_by_event_time()
    _test_mk_prop_positive_tgt_rets()


        
 


    
if __name__ == "__main__":
    run_tests()




