import os, sys
import pandas


ROOT_DIR = os.path.abspath(os.path.dirname('__file__'))
DATA_DIR = os.path.join(ROOT_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw_data')

def load_clean_bank_data(infile):
    """
    infile: str, name of file or path to file
    perform basic string processing 
    return pandas DataFrame
    """
    infile = os.path.join(RAW_DATA_DIR, infile)
    df = pd.read_csv(infile, delimiter=";")

    # drop duplicates
    df = df.drop_duplicates()

    df.columns = df.columns.str.replace(".", "_")
    df['job'] = df.job.apply(lambda x: x.replace(".",""))
    df = df.applymap(lambda x: x.replace(".", "_") if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.replace("-", "_").strip() if isinstance(x, str) else x)
    return df


def save_csv_pickle(outfile, df):
    """
    outfile: filename without extension
    df: pandas Dataframe
    saves df to csv and pkl
    """
    df.to_csv(outfile + ".csv")
    df.to_pickle(outfile + ".pkl")


def cols():
    return {
    "client_columns" : ['age', 'job', 'marital', 'education', 
    
    'default', 'housing', 'loan', "y"],
    
    "last_contact_current_campaign" : ['contact', 'month', 'day_of_week'],
    
    "other": ['campaign', 'previous', 'pdays', 'poutcome', 'duration'],
    
    "social_economic_context" : ['emp_var_rate', 'cons_price_idx', 
        'cons_conf_idx', 'euribor3m', 'nr_employed']
}


def save_categories(df, cols, DATA_DIR=DATA_DIR, return_df=False):
    """
    df: pandas DafaFrane

    cols: dict; 
        keys: str, names of files
        values: list of strings; names of columns in df
        cols is used to select groups of columns in df
        and save them to different files (in csv and pkl format)

    DATA_DIR = str, default data directory
    """
    for outfile, cols in list(cols().items()):
        out = os.path.join(DATA_DIR, outfile)
        save_csv_pickle(out, df[cols])
    if return_df:
        return df   


def main(infile, outfile):
    df = load_clean_bank_data(infile)
    # save entire dataframe
    save_csv_pickle(os.path.join(DATA_DIR, outfile), df)
    # same seprate dataframes - 1/table
    save_categories(df, cols, DATA_DIR=DATA_DIR, return_df=False)


if __name__ == "__main__":
    infile = "bank-additional-full.csv"
    outfile = "bank"
    sys.exit(main(infile, outfile))