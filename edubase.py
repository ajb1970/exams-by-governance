from pandas import DataFrame, read_csv, Index
from pandas.api.types import is_numeric_dtype
from datetime import date, datetime
from os import listdir, remove, path, mkdir

csv_dir = "csv"
output_dir = "csv/edubase"
output_filenames = {
    "edubase": f"{output_dir}/edubase.csv",
    "lookup": f"{output_dir}/lookup.csv",
    "statefunded": f"{output_dir}/statefunded.csv",
    "group_links": f"{output_dir}/group_links.csv",
    "groups": f"{output_dir}/groups.csv",
    "academies": f"{output_dir}/academies.csv",
    }

# check output dir exists, if not create it
for folder in [csv_dir, output_dir]:
    if not(path.exists(folder)):
        mkdir(folder)

# check output files exist
if (
        path.exists(output_filenames['edubase']) and
        path.exists(output_filenames['statefunded']) and
        path.exists(output_filenames['group_links']) and
        path.exists(output_filenames['groups']) and
        path.exists(output_filenames['academies']) and
        path.exists(output_filenames['lookup'])
        ):
    make_files = False
else:
    make_files = True

# check if edubase file created today
if not(make_files):
    for filename in output_filenames:
        date_file_created = datetime.fromtimestamp(
            path.getmtime(output_filenames[filename])
            )
        date_today = date.today()
        if (
                date_today.year != date_file_created.year or
                date_today.month != date_file_created.month or
                date_today.day != date_file_created.day
                ):
            make_files = True

if make_files:
    base_url = "https://ea-edubase-api-prod.azurewebsites.net/edubase/\
downloads/public/"
    todays_date = date.today().strftime("%Y%m%d")
    edubase_url = f"{base_url}edubasealldata{todays_date}.csv"
    links_url = f"{base_url}links_edubasealldata{todays_date}.csv"
    statefunded_url = f'{base_url}edubaseallstatefunded{todays_date}.csv'
    group_links_url = f'{base_url}alllinksdata{todays_date}.csv'
    groups_url = f'{base_url}allgroupsdata{todays_date}.csv'
    academies_url = f'{base_url}academiesmatmembership{todays_date}.csv'

    #try to download new edubase files, if fail use old files
    cant_make_files = False
    try:
        # download edubase
        edubase = read_csv(
            edubase_url,
            encoding="latin",
            index_col=0,
            low_memory=False,
            dayfirst=True,
            parse_dates=[
                'OpenDate', 
                'CloseDate',
                'CensusDate',
                'LastChangedDate',
                'DateOfLastInspectionVisit',
                ],
            )
        # download open state funded schools
        statefunded = read_csv(
            statefunded_url,
            encoding="latin",
            index_col=0,
            low_memory=False,
            dayfirst=True,
            parse_dates=['OpenDate'],
            )
        # download edubase links
        links = read_csv(links_url, encoding="latin", usecols=[0,1,3])
        # download group_links
        group_links = read_csv(
            group_links_url,
            encoding="latin",
            low_memory=False,
            dayfirst=True,
            parse_dates=[
                'Closed Date', 
                'Open date', 
                'Joined date', 
                'Incorporated on (open date)'
                ],
            )
        # download groups
        groups = read_csv(
            groups_url,
            encoding="latin",
            index_col=0,
            low_memory=False,
            dayfirst=True,
            parse_dates=[
                'Closed Date', 
                'Open date', 
                'Incorporated on (open date)'
                ],
            )
        # download academies and MATs
        academies = read_csv(
            academies_url,
            encoding="latin",
            low_memory=False,
            parse_dates=[
                'Establishment OpenDate', 
                'Establishment CloseDate', 
                'Group Open Date',
                'Group Closed Date', 
                'Date Joined Group', 
                'Date Left Group',
                ],
            )
    except:
        print(f"Edubase files download failed. Using last successful download \
from {date_file_created.strftime('%A, %d %B %Y')}.")
        cant_make_files = True

if make_files and not(cant_make_files):
    # process links csv to cut to just unique successors
    links = links.loc[links.LinkType=="Successor"].copy()
    links.drop(columns=["LinkType"], inplace=True)
    links.drop_duplicates(subset="LinkURN", keep=False, inplace=True)
    links.drop_duplicates(subset="URN", keep=False, inplace=True)
    links.rename(columns={"LinkURN": "NEW_URN"}, inplace=True)
    links.set_index("URN", inplace=True)
    links = links.loc[links["NEW_URN"] > links.index]

    # create inital lookup table
    lookup = DataFrame(
        {"LATEST_URN": edubase.index},
        index = Index(data=edubase.index, name="OLD_URN")
        )

    # cycle through links file to update lookup table
    # until there are no more successors
    matches=1
    while matches>0:
        lookup = lookup.merge(
            links,
            how="left",
            left_on="LATEST_URN",
            right_index=True
            )
        matches = lookup["NEW_URN"].count()
        lookup.loc[
            lookup["NEW_URN"].notnull(),
            "LATEST_URN"
            ] = lookup["NEW_URN"]
        lookup.drop(columns=["NEW_URN"],inplace=True)

    # delete links DataFrame
    del(links)

    # delete files in output_dir
    for f in listdir(output_dir):
        file_path = f"{output_dir}/{f}"
        if path.isfile(file_path):
            if file_path[-4:].lower() == ".csv":
                remove(file_path)

    # save CSVs to output_dir
    edubase.to_csv(output_filenames["edubase"])
    statefunded.to_csv(output_filenames["statefunded"])
    lookup.to_csv(output_filenames["lookup"])
    group_links.to_csv(output_filenames["group_links"])
    groups.to_csv(output_filenames["groups"])
    academies.to_csv(output_filenames["academies"])    

else:
    edubase = read_csv(
        output_filenames["edubase"],
        index_col=0,
        low_memory=False,
        parse_dates=[
            'OpenDate', 
            'CloseDate',
            'CensusDate',
            'LastChangedDate',
            'DateOfLastInspectionVisit',
            ],
        )
    statefunded = read_csv(
        output_filenames["statefunded"],
        index_col=0,
        low_memory=False,
        parse_dates=['OpenDate'],
        )
    group_links = read_csv(
        output_filenames["group_links"],
        low_memory=False,
        parse_dates=[
            'Closed Date', 
            'Open date', 
            'Joined date', 
            'Incorporated on (open date)'
            ],
        )
    groups = read_csv(
        output_filenames["groups"],
        index_col=0,
        low_memory=False,
        parse_dates=[
            'Closed Date', 
            'Open date', 
            'Incorporated on (open date)'
            ],
        )
    academies = read_csv(
        output_filenames["academies"],
        low_memory=False,
        parse_dates=[
            'Establishment OpenDate', 
            'Establishment CloseDate', 
            'Group Open Date',
            'Group Closed Date', 
            'Date Joined Group', 
            'Date Left Group',
            ],
        )
    lookup = read_csv(
        output_filenames["lookup"],
        index_col=0,
        )

def update_urn(
        df,
        urn_index=True,
        urn_col=None,
        unique_urn_output=True,
        drop_old_urn=True
        ):
    """

    Parameters
    ----------
    df : Pandas DataFrame
        DfE dataset with URN as index or among columns.
    urn_index : Boolean, optional
        Whether the URN is contained in the index. The default is True.
    urn_col : str, optional
        The name of the column containing the URN. The default is None.
    unique_urn_output : Boolean, optional
        Whether to deduplicate the output DataFrame. The default is True.
    drop_old_urn : Boolean, optional
        Whether to drop the orginal URN column. The default is True.
        If False, the old column with the URN is named "OLD_URN".

    Returns
    -------
    DataFrame with latest URN value for schools.

    """
    if not(urn_index) and urn_col is None:
        raise ValueError(
            'You must identify the location of URN in the DataFrame either the\
index or a column.'
            )
    if urn_index:
        if not(is_numeric_dtype(df.index)):
            raise ValueError('URN index must be numeric.')
    else:
        if not(is_numeric_dtype(df[urn_col])):
            raise ValueError('URN column must be numeric.')
    for column in ['LATEST_URN', 'UPDATE_URN_OPEN_CLOSE_STATUS', 'OLD_URN']:
        if column in df.columns:
            raise ValueError(
                f"Input DataFrame must not contain column named '{column}'."
                )

    if urn_index:
        output_df = df.merge(
            lookup,
            how='left',
            left_index=True,
            right_index=True
            )
    else:
        output_df = df.merge(
            lookup,
            how='left',
            left_on=urn_col,
            right_index=True
            )

    data_len = len(output_df)
    output_df.dropna(subset=['LATEST_URN'], inplace=True)
    rows_dropped = data_len - len(output_df)
    if rows_dropped>0:
        print(f"Rows with invalid URNs dropped: {rows_dropped}")

    if unique_urn_output:
        # order schools by open/close status. Open is last
        status_map = {
            'Open': 3,
            'Closed': 0,
            'Open, but proposed to close': 2,
            'Proposed to open': 1,
            }
        edu = edubase[['EstablishmentStatus (name)']].copy()
        edu['UPDATE_URN_OPEN_CLOSE_STATUS'] = (
            edu['EstablishmentStatus (name)'].map(status_map)
            )
        edu.drop(columns=['EstablishmentStatus (name)'], inplace=True)
        output_df = output_df.merge(
            edu,
            how='left',
            left_on='LATEST_URN',
            right_index=True,
            )
        if urn_index:
            output_df.sort_index(inplace=True)
            output_df.sort_values(
                ['UPDATE_URN_OPEN_CLOSE_STATUS'],
                inplace=True
                )
        else:
            output_df.sort_values(
                ['UPDATE_URN_OPEN_CLOSE_STATUS', urn_col],
                inplace=True
                )
        data_len = len(output_df)
        output_df.drop_duplicates(
            subset='LATEST_URN',
            inplace=True,
            keep='last'
            )
        print(f"Duplicates dropped from data: {data_len-len(output_df)}")
        output_df.drop(columns=['UPDATE_URN_OPEN_CLOSE_STATUS'], inplace=True)

    if not(drop_old_urn):
        if urn_index:
            output_df['OLD_URN'] = output_df.index
        else:
            output_df['OLD_URN'] = output_df[urn_col]

    if urn_index:
        new_index = Index(output_df['LATEST_URN'], name='URN')
        output_df.index = new_index
        output_df.drop(columns=['LATEST_URN'], inplace=True)
    else:
        output_df.drop(columns=[urn_col], inplace=True)
        output_df.rename(columns={'LATEST_URN': urn_col}, inplace=True)

    return output_df