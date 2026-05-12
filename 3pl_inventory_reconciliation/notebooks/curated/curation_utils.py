import re
import pandas as pd
from pathlib import Path
import os
import numpy as np


def get_3pl_combined_key(file_path):
    path_obj = Path(file_path)
    print(file_path)
    print(path_obj.parts)
    pl_number = path_obj.parts[-2]
    return pl_number.replace(' ', '_')


def get_pl_number(file_path):
    path_obj = Path(file_path)
    folder_name = path_obj.parts[-2]
    match = re.search(r'^\d+', folder_name)
    if match:
        return match.group(0)  # Return the first matched number
    else:
        return None


def add_pl_details(df, file_path, plant_name_mapping_df, pl_type_mapping_df):
    path_obj = Path(file_path)
    pl_number = path_obj.parts[-2]
    match = re.search(r'^\d+', pl_number)
    pl_number = match.group(0)
    df.columns = df.columns.str.lower()
    df['3PL'] = pl_number  # Return the first matched number
    pl_type_mapping_dict = pl_type_mapping_df.set_index('3PL')['3PL_Type'].to_dict()
    df['3PL_Type'] = pl_type_mapping_dict[pl_number]
    plant_name_mapping_dict = plant_name_mapping_df.set_index('plant_number')['plant_name'].to_dict()
    df['3PL_Name'] = plant_name_mapping_dict[pl_number]
    return df


def add_metadata(df, file_path):
    file_name = os.path.basename(file_path)

    df['File_Name'] = file_name
    df['Date_Processed'] = pd.Timestamp.today().strftime('%Y-%m-%d')

    path_obj = Path(file_path)
    year = path_obj.parts[-5]
    quarter = path_obj.parts[-4]
    df['Year'] = f"{year}"
    df['Quarter'] = f"{quarter}"
    df['Has_Error'] = False
    return df



def process_header_mapping(header_mapping_df):
    header_mapping_df = header_mapping_df.dropna(subset=['3PL'])
    header_mapping_df['Sheet_Name'] = header_mapping_df['Sheet_Name'].replace('nan', np.nan)
    header_mapping_df['3PL'] = header_mapping_df['3PL'].astype('int').astype('str')
    header_mapping_df['combined_key'] = header_mapping_df.apply(
        lambda row: f"{row['3PL']}_{row['Sheet_Name'].replace(' ', '_')}" if pd.notna(
            row.get('Sheet_Name')) and row.get('Sheet_Name') else row['3PL'],
        axis=1
    )
    header_mapping_dict = (
        header_mapping_df.groupby('combined_key')
        .apply(lambda g: dict(zip(g['3PL_Column_Header'], g['Gilead_Column_Header '])))
        .to_dict()
    )
    return header_mapping_dict


def clean_all_string_columns(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace(u'\xa0', '') if hasattr(df[col], 'str') else df[col]
    return df



def additional_preprocessing(df, quantity_processed):
    # df = df[df['3PL_Material_Code'].isna() | df['3PL_Material_Code'].astype(str).str.contains(r'\d')]
    df['3PL_Material_Code'] = df['3PL_Material_Code'].str.replace('\.0$', '', regex=True)
    if not quantity_processed:
        #df['3PL_Quantity'] = df['3PL_Quantity'].str.replace(r'[^\d\.]', '', regex=True)
        df['3PL_Quantity'] = df['3PL_Quantity'].str.replace(r'[^\d\.\-]', '',regex=True)
    df = df[df['3PL_Quantity'].isna() | pd.to_numeric(df['3PL_Quantity'], errors='coerce').notna()]
    df['3PL_Quantity'] = df['3PL_Quantity'].astype(float)
    materials_with_batch = df[df['3PL_Batch_Number'].notna()]['3PL_Material_Code'].unique()
    df = df[df['3PL_Batch_Number'].notna() | ~df['3PL_Material_Code'].isin(materials_with_batch)]
    return df


def map_filter_pl_df(df, pl_number, header_mapping_df):
    header_mapping_df['Sheet_Name'] = header_mapping_df.groupby('3PL')['Sheet_Name'].ffill().str.lower()
    header_mapping_df['3PL_Column_Header'] = header_mapping_df['3PL_Column_Header'].str.lower()
 
    header_mapping = process_header_mapping(header_mapping_df)

    column_mapping = header_mapping[pl_number.lower()]

    complex_mappings = {}
    simple_mapping = {}
    for source_col, target_col in column_mapping.items():
        if ',' in source_col and target_col == '3PL_Batch_Number':
            source_cols = [re.sub(r'\s+', ' ', col.replace(u'\xa0', ' ')).strip() for col in source_col.split(',')]
            complex_mappings[target_col] = source_cols
        elif ',' in source_col and target_col == '3PL_Material_Code':
            source_cols = [re.sub(r'\s+', ' ', col.replace(u'\xa0', ' ')).strip() for col in source_col.split(',')]
            complex_mappings[target_col] = source_cols
        elif '+' in source_col and target_col == '3PL_Quantity':
            source_cols = [re.sub(r'\s+', ' ', col.replace(u'\xa0', ' ')).strip() for col in source_col.split('+')]
            complex_mappings[target_col] = source_cols
        else:
            simple_mapping[re.sub(r'\s+', ' ', source_col.replace(u'\xa0', ' ')).strip()] = target_col 

    df = df.rename(columns=simple_mapping)

    quantity_processed = False
    for target_col, source_cols in complex_mappings.items():
        if target_col == '3PL_Quantity':
            for col in source_cols:
                if col in df.columns:  # Make sure the column exists 
                    df[col] = df[col].str.replace(r'[^\d\.\-]', '', regex=True).astype(float)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    
            valid_cols = [col for col in source_cols if col in df.columns]
            if valid_cols:
                df[target_col] = df[valid_cols].sum(axis=1)
            else:
                df[target_col] = np.nan
            quantity_processed = True

        if target_col == '3PL_Batch_Number':
            df[target_col] = np.nan

            for source_col in source_cols:
                if source_col in df.columns:
                    mask = df[target_col].isna() & df[source_col].notna()
                    df.loc[mask, target_col] = df.loc[mask, source_col]

        if target_col == '3PL_Material_Code':
            df[target_col] = np.nan
            
            for source_col in source_cols:
                if source_col in df.columns:
                    df.loc[~df[source_col].astype(str).str.contains(r'\d', na=False), source_col] = np.nan
                    mask = df[target_col].isna() & df[source_col].notna()
                    df.loc[mask, target_col] = df.loc[mask, source_col]

    # Validation check for required columns
    required_columns = ['3PL_Material_Code', '3PL_Batch_Number', '3PL_Quantity']
    missing_columns = []

    for col in required_columns:
        if col not in df.columns or df[col].isna().all():
            missing_columns.append(col)

    if missing_columns:
        raise ValueError(f"Validation failed: Missing or empty required columns: {', '.join(missing_columns)}")

    if '3PL_UOM' not in df.columns:
        df['3PL_UOM'] = None

    df = df[['3PL', '3PL_Name', '3PL_Material_Code', '3PL_Batch_Number', '3PL_Quantity', '3PL_UOM', '3PL_Type']]

    df = additional_preprocessing(df, quantity_processed)

    return df


def aggregate_quantities(df):
    main_group_cols = ['3PL', '3PL_Material_Code', '3PL_Batch_Number']
    group_cols = [col for col in df.columns if col not in ['3PL_Quantity']]

    valid = df[df[main_group_cols].notna().all(axis=1)]  # rows with no nulls in any group key
    invalid = df[~df[main_group_cols].notna().all(axis=1)]

    grouped = valid.groupby(group_cols, dropna=False, as_index=False)['3PL_Quantity'].sum()

    df = pd.concat([grouped, invalid], ignore_index=True)
    return df


def map_uom_master(df, uom_master_df):

    if df['3PL_UOM'].isna().all():
        print("\t3PL_UOM column is empty. Skipping additional conversion.")
        return df

    if not df['Conversion_Factor'].isna().any():
        print("\tNo rows without Conversion Factor values found. Skipping conversion.")
        return df

    df['3PL_UOM_upper'] = df['3PL_UOM'].str.upper()

    uom_standardization = {
        'GM': ['GRAM', 'GRAMS', 'GR', 'GRM', 'G', ],
        'KG': ['KILOGRAM', 'KILOGRAMS', 'KILO', 'KGS']
    }

    for standard, variations in uom_standardization.items():
        all_variations = variations + [standard]
        df.loc[df['3PL_UOM_upper'].isin(all_variations), '3PL_UOM_upper'] = standard

    uom_master_df['conversion_factor'] = uom_master_df['conversion_factor'].astype(float)
    df = df.merge(uom_master_df[['matnr', 'alternate_uom', 'gilead_uom', 'conversion_factor']].drop_duplicates(),
                  how='left',
                  left_on=['Gilead_Material_Code', '3PL_UOM'],
                  right_on=['matnr', 'alternate_uom'])

    # mask = df['conversion_factor'].notna()
    mask = df['conversion_factor'].notna() & df['Conversion_Factor'].isna()
    if mask.any():
        df.loc[mask, 'Conversion_Factor'] = df.loc[mask, 'conversion_factor']

    df = df.drop(['3PL_UOM_upper', 'matnr', 'alternate_uom', 'gilead_uom', 'conversion_factor'], axis=1)
    df['Conversion_Factor'] = df['Conversion_Factor'].astype(float)
    return df


def map_uom_and_convert(df, uom_mapping_df, uom_master_df):
    uom_mapping_df = uom_mapping_df.dropna(subset=['3PL_Part'])
    uom_mapping_df = clean_all_string_columns(uom_mapping_df)

    # TODO possibily remove the below conversion
    uom_mapping_df['Conversion_Factor'] = uom_mapping_df['Conversion_Factor'].astype('float')
    df = df.merge(uom_mapping_df[['Plant_Number', '3PL_Part', 'Gilead_UOM', 'Conversion_Factor']].drop_duplicates(),
                  how='left',
                  left_on=['3PL', '3PL_Material_Code'],
                  right_on=['Plant_Number', '3PL_Part'])
    df['Gilead_UOM'] = df['Gilead_UOM'].astype('str')
    df['Conversion_Factor'] = df['Conversion_Factor'].astype(float)

    df = map_uom_master(df, uom_master_df)

    mask = df['Conversion_Factor'].notna()
    df['3PL_Converted_Quantity'] = df['3PL_Quantity']
    df.loc[mask, '3PL_Converted_Quantity'] = df.loc[mask, '3PL_Quantity'] * df.loc[mask, 'Conversion_Factor']

    df = df.drop(columns=['3PL_Part', 'Plant_Number'])

    return df


def map_material_code(df, item_mapping_df, material_master_df):
    pd.set_option('display.max_rows', None)
    item_mapping_df = clean_all_string_columns(item_mapping_df)
    # print("After cleaning",item_mapping_df.head())
    item_mapping_df = item_mapping_df.dropna(subset=['3PL_Part'])
    # print("After Cleaning null from item mapping",item_mapping_df)

    df = df.merge(
        item_mapping_df[['Plant_Number', '3PL_Part', 'Material_Number', 'Material_Description']].drop_duplicates(),
        how='left',
        left_on=['3PL', '3PL_Material_Code'],
        right_on=['Plant_Number', '3PL_Part'])

    mask = df['Material_Number'].notna()
    df.loc[mask, 'Gilead_Material_Code'] = df.loc[mask, 'Material_Number']

    df = df.drop(columns=['Plant_Number', '3PL_Part', 'Material_Number', 'Material_Description'])

    valid_material_codes = set(material_master_df['matnr'].dropna().unique())

    material_master_df = material_master_df.drop_duplicates()
    df = df.merge(material_master_df,
                  how='left',
                  left_on='3PL_Material_Code',
                  right_on='matnr')

    mask = (df['matnr'].notna()) & (df['Gilead_Material_Code'].isna())
    df.loc[mask, 'Gilead_Material_Code'] = df.loc[mask, 'matnr']

    df = df.drop(columns=['matnr'])

    # Check if Gilead_Material_Code exists in material master
    mask_not_in_master = ~df['Gilead_Material_Code'].isin(valid_material_codes) & df['Gilead_Material_Code'].notna()
    df.loc[mask_not_in_master, 'Validation_Remark'] = 'Mapped Material Code Not In Material master'
    df.loc[mask_not_in_master, 'Has_Error'] = True

    # Handle completely missing material codes
    mask_invalid_material = df['Gilead_Material_Code'].isna()
    df.loc[mask_invalid_material, 'Validation_Remark'] = 'Invalid 3PL Material Code'
    df.loc[mask_invalid_material, 'Has_Error'] = True

    # Handle completely missing material codes
    mask_null_material = df['3PL_Material_Code'].isna()
    df.loc[mask_null_material, 'Validation_Remark'] = 'Material Code is NULL in 3PL file'
    df.loc[mask_null_material, 'Has_Error'] = True

    df['Gilead_Material_Code'] = df['Gilead_Material_Code'].astype(str)

    return df


def map_lot_no(df, lnmdf):
    lnmdf = lnmdf.astype(str)
    lnmdf['atwrt'] = lnmdf['atwrt'].str.lstrip('0')

    lnmdf = lnmdf[(lnmdf['atinn'] == '0000000828') | (lnmdf['atinn'] == '0000000819')]

    df = df.merge(lnmdf,
                  how='left',
                  left_on=['Gilead_Material_Code', '3PL_Batch_Number'],
                  right_on=['matnr', 'atwrt'])

    mask = df['charg'].notna()  # & (df['Gilead_Batch_Number'].isna())
    df.loc[mask, 'Gilead_Batch_Number'] = df.loc[mask, 'charg']

    df = df.drop(columns=['charg', 'matnr', 'atinn', 'atwrt'])
    return df


def map_lot_no_wildcard(df, lot_number_master_df, lnmdf, sapdf):
    lot_number_master_df = lot_number_master_df.drop_duplicates()

    df = df.merge(lot_number_master_df,
                  how='left',
                  left_on=['Gilead_Material_Code', '3PL_Batch_Number'],
                  right_on=['matnr', 'charg'])

    mask = df['charg'].notna()
    df.loc[mask, 'Gilead_Batch_Number'] = df.loc[mask, 'charg']

    already_matched = df['Gilead_Batch_Number'].notna()

    df = df.drop(columns=['matnr', 'charg'])

    lnmdf = lnmdf.astype(str)
    lnmdf['atwrt'] = lnmdf['atwrt'].str.lstrip('0')

    # lnmdf = lnmdf[(lnmdf['atinn'] == '0000000828') | (lnmdf['atinn'] == '0000000819')]
    lnmdf = lnmdf.drop_duplicates(['charg', 'matnr', 'atwrt'])

    matches = []
    unmatched_df = df[~already_matched].reset_index(drop=True)
    matched_df = df[already_matched].reset_index(drop=True)

    # Process only unmatched rows
    for left_idx, left_row in unmatched_df.iterrows():
        match_found = False
        potential_matches = lnmdf[lnmdf['matnr'] == left_row['Gilead_Material_Code']]

        for right_idx, right_row in potential_matches.iterrows():
            if str(left_row['3PL_Batch_Number']) in str(right_row['atwrt']):
                combined = {**left_row.to_dict(), **right_row.to_dict()}
                matches.append(combined)
                match_found = True
                break

        if not match_found:
            right_nulls = {col: None for col in lnmdf.columns if col not in unmatched_df.columns}
            combined = {**left_row.to_dict(), **right_nulls}
            matches.append(combined)

    # For already matched rows, just add the nulls for lnmdf columns
    if not matched_df.empty:
        for _, matched_row in matched_df.iterrows():
            right_nulls = {col: None for col in lnmdf.columns if col not in matched_df.columns}
            combined = {**matched_row.to_dict(), **right_nulls}
            matches.append(combined)

    # Reconstruct the dataframe from matches
    df = pd.DataFrame(matches)

    mask = df['charg'].notna() & df['Gilead_Batch_Number'].isna()
    df.loc[mask, 'Gilead_Batch_Number'] = df.loc[mask, 'charg']

    df = df.drop(columns=['charg', 'matnr', 'atinn', 'atwrt'])

    # sapdf = sapdf[['Plant', 'Material_Number', 'Batch_Number']].drop_duplicates()
    # df = df.merge(sapdf,
    #               how='left',
    #               left_on=['3PL', 'Gilead_Material_Code', '3PL_Batch_Number'],
    #               right_on=['Plant', 'Material_Number', 'Batch_Number'])

    # mask = df['Batch_Number'].notna() & df['Gilead_Batch_Number'].isna()
    # df.loc[mask, 'Gilead_Batch_Number'] = df.loc[mask, 'Batch_Number']
    # df = df.drop(columns=['Plant', 'Material_Number', 'Batch_Number'])

    # Validation remarks
    mask_invalid_batch = (df['Gilead_Material_Code'].notna() & (df['Gilead_Material_Code'] != 'nan')) & df[
        'Gilead_Batch_Number'].isna()
    df.loc[mask_invalid_batch, 'Validation_Remark'] = 'Invalid 3PL Batch Number'
    df.loc[mask_invalid_batch, 'Has_Error'] = True

    mask_invalid_material_code = df['Validation_Remark'] == 'Invalid 3PL Material Code'
    mask_null_batch_q_0 = (~mask_invalid_material_code) & (df['3PL_Batch_Number'].isnull()) & (df['3PL_Quantity'] == 0)
    df.loc[mask_null_batch_q_0, 'Validation_Remark'] = 'Batch Number NULL In 3PL File'
    df.loc[mask_null_batch_q_0, 'Has_Error'] = False

    df['Gilead_Batch_Number'] = df['Gilead_Batch_Number'].astype(str)

    return df


def get_unit_cost(df, unit_cost_df):
    unit_cost_df['standard_cost_usd'] = unit_cost_df['standard_cost_usd'].astype('float')
    unit_cost_df = unit_cost_df.rename(columns={'standard_cost_usd': 'Cost'})
    df = df.merge(unit_cost_df,
                  how='left',
                  left_on=['3PL', 'Gilead_Material_Code'],
                  right_on=['plant_code', 'material_number'])
    df = df.drop(columns=['material_number', 'plant_code', 'base_uom', 'standard_cost',
                          'currency_code', 'exchange_rate', 'exchange_rate_date'])
    return df


def get_material_type(df, material_type_df):
    material_type_df = material_type_df.rename(columns={'extwg': '3PL_Material_Type'})
    df = df.merge(material_type_df,
                  how='left',
                  left_on=['Gilead_Material_Code'],
                  right_on=['matnr'])
    df = df.drop(columns=['matnr'])
    return df


def match_gilead_receipts(df, gil_receipts_df):
    gil_receipts_df = gil_receipts_df.rename(columns={'Qty': 'Gilead_Receipts', 'Plant': 'Plant_Receipts'})
    gil_receipts_df = gil_receipts_df.dropna(subset=['Plant_Receipts', 'Material', 'Batch'])

    gil_receipts_df = gil_receipts_df.astype('str')

    df = df.merge(gil_receipts_df[['Plant_Receipts', 'Material', 'Batch', 'Gilead_Receipts']],
                  how='left',
                  left_on=['Plant', 'Material Number', 'Batch Number'],
                  right_on=['Plant_Receipts', 'Material', 'Batch'])
    df = df.drop(columns=['Plant_Receipts', 'Material', 'Batch'])
    return df


def add_validation_flags(df):
    df['Has_Error'] = False
    df['Validation_Remark'] = ''

    mask_invalid_material = df['Gilead_Material_Code'].isna()
    df.loc[mask_invalid_material, 'Validation_Remark'] = 'Invalid Material Code'

    mask_invalid_batch = df['Gilead_Material_Code'].notna() & df['Gilead_Batch_Number'].isna()
    df.loc[mask_invalid_batch, 'Validation_Remark'] = 'Unmatched 3PL Batch Number'
    df.loc[mask_invalid_material | mask_invalid_batch, 'Has_Error'] = True

    mask_null_batch_q_0 = (df['3PL_Batch_Number'].isnull()) & (df['3PL_Quantity'] == 0)
    df.loc[mask_null_batch_q_0, 'Validation_Remark'] = 'Batch Number NULL In 3PL File'
    df.loc[mask_null_batch_q_0, 'Has_Error'] = False
    df['Has_Error'] = df['Has_Error'].fillna(False)
    return df




