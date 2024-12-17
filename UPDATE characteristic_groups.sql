UPDATE characteristic_groups
SET characteristicgroup = CASE
    WHEN characteristicname ILIKE ANY (ARRAY[
        '1,2-Dichloroethane-d4', '1,3-Butadiene', '1,3-Dioxane', '1,4-Dioxane', '1,2,4-Trimethylbenzene', 
        '1,3-Dioxolane', '1-Bromo-3-chloropropane-d6', 'Diazepam-D5', 'Ethyl acetate', 'Ethylbenzene', 
        'Methyl acetate', 'Methyl bromide', 'Methylene chloride', 'Isobutyle acetate', 'Isopropanol', 
        'Dimethoxymethane', 'Ethane, 1-chloro-1,1-difluoro-', 'D-Limonene', 'Ethanol,2-(4-nonylphenoxy)-', 
        'p-Cresol', 'p-Cumylphenol', 'p-Dichlorobenzene', 'Pentanal', 'Pentane', 'Propyl acetate', 
        'Styrene', 'Toluene-d8', 'Vinyl chloride', 'Di(2-ethoxylhexyl) phthalate', 'Dibenz[a,h]anthracene', 
        'Dibenzothiophene', 'Fluorant
        'Inorganic nitrogen (nitrate and nitrite)', 'Orthophosphate', 'Pentachloroanisole', 'Pentachlorobiphenyl', 
        'Perylene', 'Phenanthrene', 'Phenanthridine', 'Phenazopyridine', 'Phthalazinone', 'p-Octylphenol', 
        'Triphenyl phosphate', 'Nutrients', 'Phosphorus', 'Phosphorus, Particulate Organic', 
        'Soluble Reactive Phosphorus (SRhene', 'Fluorene', 'Nonylphenol diethoxylate', 'Lipids', 'Organic carbon', 
        'Organic nitrogen', 'Organic phosphorus', 'Nutrients', 'Nitrogen', 'Nitrate + Nitrite', P)', 'Total Nitrogen, mixed forms (NH3), (NH4), organic, (NO2) and (NO3)'
    ]) THEN 'Organic Compounds'

    UPDATE c_category SET characteristic_group = CASE WHEN characteristicname ILIKE ANY (ARRAY[
        "1H-Pyrazole-3-carboxamide", "5-amino-1-[2,6-dichloro-4-(trifluoromethyl)",
        "phenyl]-4-[(trifluoromethyl)sulfinyl]-", "2-(1-Hydroxyethyl)-6-methylaniline", "Methyl-2-pentanol",
        "Acetaminophen", "Acetaminophen-d3", "Acetochlor-d11", "Acetochlor ESA", "Acetochlor OA",
        "Acetochlor sulfinylacetic acid", "Acetophenone", "Acibenzolar-S-methyl", "Aldicarb",
        "Aldicarb sulfone", "Aldicarb sulfoxide", "Aldrin", "Alachlor-d13", "Alachlor ESA",
        "Alachlor OA", "Alachlor sulfinylacetic acid", "Amphetamine", "Amphetamine-d6", "Anthracene",
        "Benz[a]anthracene", "Benzo[b]fluoranthene", "Benzo[k]fluoranthene", "Benzo[e]pyrene",
        "Benzo[ghi]perylene", "Benzophenone", "Benzovindiflupyr", "Bromochloromethane", "Chloroform",
        "Chloromethane", "Carbon disulfide", "Carbon tetrachloride", "Cyclohexanone", "Cumene",
        "Cyclopental[df]phenanthrene", "Citalopram", "Clonidine", "Chlordane", "Chlorthal-dimethyl",
        "Chlorthal-Monomethyl", "Hydroxyhexazinone A", "Hydroxy molinate", "Hydroxyimidacloprid",
        "Tolytriazole", "Ametryn", "Azinphos-methyl", "Azinphos-methyl oxygen analog", "Asulam",
        "Captan", "Carbaryl-d7", "Carbendazim", "Carbofuran", "Chlorantraniliprole", "Chlorfenapyr",
        "Chloriumuron-ethyl", "Chlorothalonil", "Chlorsulfuron", "Chloronhexadecafluoro-3-oxanonane-1-sulfonate",
        "Cyhalofop butyl", "Cyprodinil", "Cyproconazole", "Cyazofamid", "Cyclaniliprole", "Deethylatrazine-d6",
        "Deethylhydroxyatrazine", "Demethylfluometuron", "Demethyl hexazinone B", "Deltamethrin", "Dicamba",
        "Diazinon", "Dieldrin", "Dimethachlor sulfonic acid", "Dimethenamid", "Dimethoate", "Dithiopyr",
        "Fluazinam", "Fluopicolide", "Fipronil", "Fipronil Desulfinyl", "Fipronil sulfonate", "Fipronil Sulfide",
        "Flonicamid", "Imazalil", "Imidacloprid desnitro", "Nicosulfuron", "Oxadiazon", "Oxamyl", "Pebulate",
        "Penoxsulam", "Pentachloronitrobenzene", "Pentachlorophenol", "Penthiopyrad", "Phorate",
        "Phorate O.A.", "Phorateoxonsulfoxide", "Profenofos", "Prometryn", "Propazine", "Pyracclostrobin",
        "Resmethrin", "Sulfentrazone", "Thiacloprid", "Thiamethoxam", "Trichlorobiphenyl", "Trichloroethylene"
    ]) THEN 'Pesticides and Herbicides' END;


	WHEN characteristicname ILIKE ANY (ARRAY[
        'Abacavir', 'Albuterol', 'Albuterol-d9', 'Antipyrine', 'Caffeine', 'Caffine-d9', 
        'Caffeine-trimethyl-13C3', 'Codeine-d6', 'Androsterone', 'Androsterone glucuronide', 
        'Androsterone sulfate', '1,2-Benzenedicarboxamide', '(1R,2S,5R)-Menthol', '16-Epiestriol-d2', 
        '1,7-Dimethylxanthine', "N1-[2-methyl-4-[1,2,2,2-tetrafluoro-1-(trifluoromethyl)ethyl]phenyl]-", 
        'Desogestrel', 'Duloxetine', 'Ethenylestradiol 3-glucuronide', 'Ethinyl estradiol', 'Loratadine', 
        'Hydrocodone', 'Norethisterone', 'Omeprazole/Esomeprazole mix', 'Paroxetine', 'Pregabalin', 
        'Tramadol', 'Penciclovir', 'Pentoxifylline', 'Prednisolone', 'Prednisone', 'Promethazine', 'Quinine', 
        'Ranitidine', 'Tamoxifen', 'Valacyclovir', 'Warfarin'
    ]) THEN 'Pharmaceuticals and Personal Care Products'

	WHEN characteristicname ILIKE ANY (ARRAY[
        "2,2',3,3',4,4',5,6-Octachlorobiphenyl", "2,2',3,3',4,4',5,6'-Octachlorobiphenyl", 
        "2,2',3,3',4,4',6,6'-Octachlorobiphenyl", "2,2',3,3',4,4',6-Heptachlorobiphenyl", 
        "2,2',3,3',4,4'-Hexachlorobiphenyl", "2,2',3,3',4,5,5',6,6'-Nonachlorobiphenyl", 
        "2,2',3,3',4,5,5',6-Octachlorobiphenyl", "2,2',3,3',4,5,5'-Heptachlorobiphenyl", 
        "2,2',3,3',4,5,6,6'-Octachlorobiphenyl", "2,2',3,3',4,5',6-Heptachlorobiphenyl", 
        "2,2',3,3',4,5',6'-Heptachlorobiphenyl", "2,2',3,3',4,5,6'-Heptachlorobiphenyl", 
        "2,2',3,3',4,5-Hexachlorobiphenyl", "2,2',3,3',4,5'-Hexachlorobiphenyl", 
        "2,2',3,3',4,6,6'-Heptachlorobiphenyl", "2,2',3,3',4,6-Hexachlorobiphenyl", 
        "2,2',3,3',4,6'-Hexachlorobiphenyl", "2,2',3,3',4-Pentachlorobiphenyl", 
        "2,2',3,3',5,5',6,6'-Octachlorobiphenyl", "2,2',3,3',5,5',6'-Heptachlorobiphenyl", 
        "2,2',3,3',5,5'-Hexachlorobiphenyl"
    ]) THEN 'Polychlorinated'

	WHEN characteristicname ILIKE ANY (ARRAY[
        "Aluminum", "Barium", "Bismuth", "Boron", "Calcium", "Copper", "Chromium-51", "Iron", 
        "Lithium", "Molybdenum", "Lead-210", "Dysprosium", "Europium", "Gadolinium", "Gallium", 
        "Polonium-210", "Potassium", "Rubidium", "Samarium", "Silver", "Strontium", "Titanium", "Uranium", "Vanadium", "Yttrium"
    ]) THEN 'Heavy Metals and Metalloids'

	WHEN characteristicname ILIKE ANY (ARRAY[
        'Absorbance at 412 nm', 'Absorbance at 440 nm', 'Absorption spectral slope (Sag)', 
        'Acidity, (H+)', 'Alkalinity, carbonate', 'Alkalinity, total', 'Biochemical oxygen demand, standard conditions', 
        'BOD, ultimate', 'Carbonaceous biochemical oxygen demand, standard conditions', 'Color', 'Cloud cover', 
        'Density at sigma-t', 'Depth', 'Depth, bottom', 'Depth of water column', 'Dissolved oxygen saturation', 
        'Light attenuation coefficient', 'Light, (PAR at depth/PAR at surface) x 100', 'Optical density', 
        'Particle size', 'Flow rate, instantaneous', 'Flow', 'Extract volume', 'Specific UV Absorbance at 254 nm', 
        'Total dissolved solids', 'Total hardness', 'Total suspended solids', 'Turbidity Field', 
        'Water surface slope longitudinally', 'Water Odor (choice list)'
    ]) THEN 'Physical and Chemical Properties'

	WHEN characteristicname ILIKE ANY (ARRAY[
        'Algae, substrate rock/bank cover (choice list)', 'BacR DNA marker', 'Coliphage', 'Cylindrospermopsin gene cyrA', 
        'Cylindrospermopsins', 'Bioluminescent yeast estrogen screen (BLYES)', 'Deoxynivalenol', 
        'Microcystin/nodularin genes mcyE/ndaF', 'Microcystis-specific Microcystin (MC) mcyE gene', 'DG3 DNA marker', 
        'HF183 DNA marker', 'Phaeophytin - Periphyton (attached)', 'Phaeophytin - Phytoplankton (suspended)', 
        'Phytoplankton biovolume', 'Phytoplankton Density', 'Saxitoxin gene sxtA', 'Saxitoxins', 
        'Total microcystins plus nodularins'
    ]) THEN 'Biological Inidcators'

	WHEN characteristicname ILIKE ANY (ARRAY[
        'Perfluoro(2-propoxypropanoate)', 'Perfluorobutanesulfonate', 'Perfluorobutanesulfonic acid', 
        'Perfluorobutanoate', 'Perfluorodecanesulfonate', 'Perfluorodecanoate', 'Perfluorooctanesulfonate', 
        'Perfluorooctanoic acid'
    ]) THEN 'Halogenated Compounds'

	WHEN characteristicname ILIKE ANY (ARRAY[
        'Precipitation', 'Precipitation 48hr prior to monitoring event (choice list)', 
        'Precipitation during activity (choice list)', 'Precipitation event duration', 
        'Relative Air Temperature (choice list)', 'Relative humidity', 'Stream condition (text)', 
        'Tide stage (choice list)', 'Weather comments (text)', 'Wind Condition (choice list)', 
        'Sediment and Soil Characteristics', 'Sediment', 'Sand', 'Silica', 'Silt', 'Solids', 
        'Total Particulate Carbon', 'Total Particulate Nitrogen', 'Total Particulate Phosphorus'
    ]) THEN 'Environmental Conditions'

END;





	
   
