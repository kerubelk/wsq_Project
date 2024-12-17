import pandas as pd

#product data set
productdf=pd.read_csv("product_composition_data_20201216.csv", encoding='latin1')

#product chemical dictionary data set
chem_dict = pd.read_csv("chemical_dictionary_20201216.csv")

product_chemical = pd.merge(chem_dict, productdf, on='chemical_id', how='inner')



