



def load_bank_data(infile, DATA_DIR=DATA_DIR):
    """
    load a pkl, csv, or json file to pandas DataFrame
    """
    in_ = os.path.join(DATA_DIR, infile)
    ext = infile.split('.')[-1]
    if ext == "pkl":
        return pd.read_pickle(in_)
    elif ext == "csv":
        return pd.read_csv(in_)
    elif ext == "json":
        return pd.read_json(in_)
    else:
        raise


def drop_columns(df):
    """
    duration: drop because duration ~ determines classification
    marital & job: drop because contain few "unknown" values
        - and unknowns are not strongly related to the target;
    """
    # duration highly determinate of target (y/subscribed)
    df.drop(columns=["duration"], inplace=True)

    # marital = 80; job= 330
#     df.drop(columns=['marital', 'job'], axis=0, inplace=True)
    return df


def drop_rows(df):
    df= df.loc[df.job != "unknown"]
    return df.loc[df.marital != 'unknown']


def impute_mode(df, impute_columns = ['education', 'housing', 'loan']):
    """
    Number to Impute with Mode:
    education: 1730, "university_degree"
    housing: 990; "yes"
    loan: 990; "no"
    """
    df.loc[df.education=="unknown", "education"] = df.education.mode().values[0]
    df.loc[df.housing=="unknown", "housing"] = df.housing.mode().values[0]
    df.loc[df.loan=="unknown", "loan"] = df.loan.mode().values[0]
    return df


def process_nulls(df):
    df = drop_columns(df)
    df = drop_rows(df)
    return impute_mode(df)