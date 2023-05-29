import pandas as pd
import os




# concat many excel files(format : {field}-1) into one
def concat_df(field, length):
    file_path = f'/Users/jiyounglim/Documents/study/공종설 네트워크 분석/files/{field}'
    dfs = list()
    for i in range(1, length + 1):
        file_name = f"{file_path}-{i}.xls"
        df = pd.read_excel(file_name)
        dfs.append(df)

    df_combined = pd.concat(dfs, axis=0)

    return df_combined


def export_to_csv(output_dir, field_dict):
    for field, length in field_dict.items():
        df = concat_df(field, length)
        # Export DataFrame to CSV
        file_name = f'{field}.csv'  # Generate the file name
        file_path = os.path.join(output_dir, file_name)  # Create the full file path
        df.to_csv(file_path, index=False)
