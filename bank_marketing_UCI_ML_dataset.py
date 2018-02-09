import sys
import pandas as pd

if __name__ == "__main__":
    infile = "bank-additional-full.csv"
    df = pd.read_csv(infile, delimiter=";")
    # df = df.drop(columns=["duration"])

    df.columns = df.columns.str.replace(".", "_")
    df['job'] = df.job.apply(lambda x: x.replace(".",""))
    df = df.applymap(lambda x: x.replace(".", "_") if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.replace("-", "_").strip() if isinstance(x, str) else x)

    month = {
        'jan': 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    day_of_week = {
        "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6
    }

    client_columns = ['age', 'job', 'marital', 'education', 'default', 'housing', 'loan']

    last_contact_current_campaign = ['contact', 'month', 'day_of_week']
    other = ['campaign', 'pdays', 'previous', 'poutcome', 'duration']
    social_economic_context = ['emp_var_rate', 'cons_price_idx', 'cons_conf_idx', 'euribor3m', 'nr_employed']

    # last contact_current_campaign: month and day of week; contact = telephone or cellular
    df['month'] = df.month.map(lambda x: month[x])
    df['day_of_week'] = df.day_of_week.map(lambda x: day_of_week[x])

    # other: poutcome, pdays, [campaign, previous, duration left as is]
    poutcome_mapper = {"success":True, "failure":False, "nonexistent": None}
    df["poutcome"] = df.poutcome.apply(lambda x: poutcome_mapper[x] if isinstance(x, str) else x)
    df.loc[df.pdays==999,'pdays'] = None

    # client_related (left alone: age, job, marital, education); default, housing, loan--> True/False/None
    bool_cols = ["default", "housing", "loan", "y"]
    bools_mapper = {"yes": True, "no": False, "unknown": None}

    for col in bool_cols:
        df[col] = df[col].apply(lambda x: bools_mapper[x] if isinstance(x, str) else x)

    df['poutcome'] = df.poutcome.apply(lambda x: bool(x) if x in [0,1] else x)
    df = df.rename(columns={"y": "target"})

    # Save all data - cleaned
    outfile = "processed_bank_data"
    df.to_csv(outfile + ".csv")
    df.to_pickle(outfile + ".pkl")

    # save client data -> so this can be loaded into script that send it to postgres db on aws
    client_df = df[client_columns + ['target']]
    outfile = "bank_client_data"
    client_df.to_csv(outfile + ".csv")
    client_df.to_pickle(outfile + ".pkl")
    sys.exit()