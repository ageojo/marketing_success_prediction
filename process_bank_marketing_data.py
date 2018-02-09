import os, sys
import pandas as pd


def save_csv_pickle(outfile, df):
	"""
	outfile should not contain file extension
	example: "filename" (not "filename.csv")
	df: pandas dataframe;
	extension will be added and files saved to csv and pickled
	"""
	df.to_csv(outfile + ".csv")
	df.to_pickle(outfile + ".pkl")


def clean(infile):
	"""
	load data, basic string cleaning for easier processing
	and to load into database
	return pandas dataframe
	"""
	df = pd.read_csv(infile, delimiter=";")
	# df = df.drop(columns=["duration"])
	df.columns = df.columns.str.replace(".", "_")
	df['job'] = df.job.apply(lambda x: x.replace(".",""))
	df = df.applymap(lambda x: x.replace(".", "_") if isinstance(x, str) else x)
	df = df.applymap(lambda x: x.replace("-", "_").strip() if isinstance(x, str) else x)
	return df


def last_contact_current_campaign(df):
	"""
	df: pandas dataframe
	replace months and days of the week with integers
	save 'last contact before current campaign'- related columns:
	(1) contact (telephone/cellular); (2) month (1-12); (3) day_of_week(0-6, Monday=0)
	save above columns into csv and pickled files
	return modified df
	"""
	month = {
    'jan': 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
	}
	day_of_week = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
	df['month'] = df.month.map(lambda x: month[x])
	df['day_of_week'] = df.day_of_week.map(lambda x: day_of_week[x])
	last_contact_current_campaign = ['contact', 'month', 'day_of_week']  #contact: telephone/cellular
	outfile = "bank_campaign_last_contact."
	save_csv_pickle(outfile, df[last_contact_current_campaign])
	return df


def other_campaign_data(df):
	"""
	df: pandas dataframe
	output: pandas dataframe
	replaces 999 (pdays) with NULL; 
	likewise for poutcome:  "nonexistent" --> NULL; success/failure _-> True/False
	saves 'other campign-related' columns (campaign, pdays, previous, poutcome) as csv and pickle files
	returns modified dataframe
	"""
	# other; campaign and previous left as is; outcome - T/F; pdays: dummy code 999-None
	poutcome_mapper = {"success":True, "failure":False, "nonexistent": None}
	df["poutcome"] = df.poutcome.apply(lambda x: poutcome_mapper[x] if isinstance(x, str) else x)
	# df['poutcome'] = df.poutcome.apply(lambda x: bool(x) if x in [0,1] else x)

	df.loc[df.pdays==999,'pdays'] = None
	other = ['campaign', 'pdays', 'previous', 'poutcome'] #'duration',

	outfile = "other_campaign_related"
	save_csv_pickle(outfile, df[other])
	return df


def client_data(df):
	"""
	return modified dataframe
	modifies client-rleated data and the target column (y) 
	containing binary "yes"/"no"/"unknown" to True/False/None
	save client specific data + target as csv and pickle file
	return modified dataframe
	"""
	# Client: age, job, marital, education;  [default, housing, loan == T/F/None]
	bool_cols = ["default", "housing", "loan", "y"]
	bools_mapper = {"yes": True, "no": False, "unknown": None}

	for col in bool_cols:
	    df[col] = df[col].apply(lambda x: bools_mapper[x] if isinstance(x, str) else x)
	
	# rename target column from "y"-> "target"
	df = df.rename(columns={"y": "target"})
	
	# client columns
	client_columns = ['age', 'job', 'marital', 'education', 'default', 'housing', 'loan']
	# include target
	client_df = df[client_columns + ['target']]

	outfile = "bank_client_data"
	save_csv_pickle(outfile, client_df)
	return df


def social_economic_context(df):
	"""
	df: dataframe
	save socioeconomic indicator data only	
	"""
	social_economic_context = ['emp_var_rate', 'cons_price_idx', 'cons_conf_idx', 'euribor3m', 'nr_employed']
	outfile = "bank_socio_economic_data"
	save_csv_pickle(outfile, df[social_economic_context])


def main(infile, outfile):
	df = clean(infile)
	df = last_contact_current_campaign(df)
	df = other_campaign_data(df)
	df = client_data(df)
	social_economic_context(df)
	# save modified dataframe with all data
	save_csv_pickle(outfile, df)
	

if __name__ == "__main__":
	infile = "bank-additional-full.csv"
	outfile = "bank_data_processed" 
	sys.exit(main(infile, outfile))

	# outfile does not contain extensionl added by `save_csv_pickle`)
	


