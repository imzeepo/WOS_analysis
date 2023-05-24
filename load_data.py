import pandas as pd

def process_files_in_directory(directory):
    stats_list = []

    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)

            # Read the file into a DataFrame
            df = pd.read_csv(file_path, low_memory=False)

            # Extract mainCategory and subCategory from the file name
            # main_category = file_name.split('_')[0]
            # sub_category = file_name.split('_')[1].split('.')[0]

            # Perform operations on the DataFrame
            coauthor = get_coauthor_matrix(df['Researcher Ids'])
            # create_coauthor_graph(coauthor, f'{main_category}_{sub_category}')
            # stats = calculate_graph_stats_for_field(f'{main_category}_{sub_category}')
            create_coauthor_graph(coauthor, f'{file_name}')
            stats = calculate_graph_stats_for_field(f'{file_name}')

            # Add mainCategory and subCategory to the stats dictionary
            # stats['mainCategory'] = main_category
            # stats['subCategory'] = sub_category

            # Add the stats to the stats_list
            stats_list.append(stats)

    # Create a DataFrame from the stats_list
    stats_df = pd.DataFrame(stats_list)

    return stats_df


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
        df_name = f'df_{field}'  # Generate the DataFrame name
        df = concat_df(field, length)
        # Export DataFrame to CSV
        file_name = f'{field}.csv'  # Generate the file name
        file_path = os.path.join(output_dir, file_name)  # Create the full file path
        df.to_csv(file_path, index=False)
