from edubase import academies, edubase
from pandas import read_csv, to_numeric, DataFrame, ExcelWriter

period_date = '2019-06-01'

phase_map = {
    'Secondary': 'Secondary',
    'Primary': 'Primary',
    'All-through': 'All-through',
    'Middle deemed secondary': 'Secondary',
    '16 plus': '16 plus',
    'Middle deemed primary': 'Primary',
    'Nursery': 'Primary',
    }
academies['Phase'] = academies['PhaseOfEducation (name)'].map(phase_map)
academies.loc[
    academies['TypeOfEstablishment (name)'].isin(
        [
            'Academy special converter',
            'Academy special sponsor led',
            'Free schools alternative provision',
            'Academy alternative provision converter',
            'Academy alternative provision sponsor led',
            'Free schools special',
            ]
        ),
    'Phase'
    ] = 'Special'
mainstream = edubase.loc[
    edubase['TypeOfEstablishment (name)'].isin(
        [
            'Voluntary aided school',
            'Community school',
            'Foundation school',
            'Voluntary controlled school',
            'City technology college',
            'Academy sponsor led',
            'Academy converter',
            'Free schools',
            'University technical college',
            'Studio schools',
            ]
        )
    ].copy()
mainstream['Phase'] = mainstream['PhaseOfEducation (name)'].map(phase_map)
mainstream.loc[
    mainstream['TypeOfEstablishment (name)'].isin(
        [
            'Pupil referral unit',
            'Foundation special school',
            'Community special school',
            'Special post 16 institution',
            'Secure units',
            ]
        ),
    'Phase'
    ] = 'Special'
mainstream_at_date = mainstream.loc[
    (
        (mainstream.OpenDate<=period_date) |
        (mainstream.OpenDate.isnull())
        ) &
    (
        (mainstream.CloseDate>period_date) |
        (mainstream.CloseDate.isnull())
        )
    ][[
        'LA (code)',
        'LA (name)',
        'EstablishmentNumber',
        'EstablishmentName',
        'Phase',
        'GOR (name)',
        ]].copy()

academies_at_date = academies.loc[
    (academies['Establishment OpenDate']<=period_date) &
    (
        (academies['Establishment CloseDate']>period_date) |
        (academies['Establishment CloseDate'].isnull())
        ) &
    (
        (academies['Group Open Date']<=period_date) |
        (academies['Group Open Date'].isnull())
        ) &
    (
        (academies['Date Joined Group']<=period_date) |
        (academies['Date Joined Group'].isnull())
        ) &
    (
        (academies['Date Left Group']>period_date) |
        (academies['Date Left Group'].isnull())
        ) &
    (
        (academies['Group Closed Date']>period_date) |
        (academies['Group Closed Date'].isnull())
        )
    ].copy()

academy_dupes = academies_at_date.loc[
    academies_at_date.URN.duplicated()
    ]
dupes_to_remove = academy_dupes.loc[
        (academy_dupes['Group Closed Date'].isnull())
        ]
academies_at_date.drop(
    index=dupes_to_remove.index,
    inplace=True
    )
academies_at_date.drop_duplicates(subset=['URN'], inplace=True)
academies_at_date.set_index('URN', inplace=True)

academies_at_date = academies_at_date[[
    'Group UID',
    'Group Type',
    'Group Name',
    ]].copy()
mainstream_at_date = mainstream_at_date.merge(
    academies_at_date,
    how='left',
    left_index=True,
    right_index=True
    )
mainstream_at_date['Governance'] = mainstream_at_date['Group Type']
mainstream_at_date.loc[
    mainstream_at_date['Governance'].isnull(),
    'Governance'
    ] = 'Maintained'

mainstream_at_date['Governance organisation'] = (
    mainstream_at_date['Group Name']
    )
mainstream_at_date.loc[
    mainstream_at_date['Governance organisation'].isnull(),
    'Governance organisation'
    ] = mainstream_at_date['LA (name)'] + ' LA'

ks4 = read_csv(
    'data/exam_accounts_workforce/2018-2019/england_ks4final.csv',
    usecols = [
         'URN',
         'P8PUP_FSM6CLA1A',
         'P8MEA_FSM6CLA1A',
         'P8PUP_NFSM6CLA1A',
         'P8MEA_NFSM6CLA1A',
         ],
    na_values = ['NA', 'NP', 'SUPP', 'LOWCOV', 'NE'],
    )
ks4.dropna(subset=['URN'], inplace=True)
ks4.rename(
    columns={
        'P8PUP_FSM6CLA1A':
            'Number of disadvantaged pupils',
        'P8MEA_FSM6CLA1A':
            'Progress 8 - disadvantaged pupils',
        'P8PUP_NFSM6CLA1A':
            'Number of non-disadvantaged pupils',
        'P8MEA_NFSM6CLA1A':
            'Progress 8 - non-disadvantaged pupils',
        },
    inplace=True
    )
ks4.set_index('URN', inplace=True)
ks4.dropna(how='any', inplace=True)

secondary = mainstream_at_date.merge(
    ks4,
    how='inner',
    left_index=True,
    right_index=True
    )
secondary['Progress 8 total non-disadvantaged'] = (
    secondary['Number of non-disadvantaged pupils'] *
    secondary['Progress 8 - non-disadvantaged pupils']
    )
secondary['Progress 8 total disadvantaged'] = (
    secondary['Number of disadvantaged pupils'] *
    secondary['Progress 8 - disadvantaged pupils']
    )
selective_urns = edubase.loc[
    edubase['AdmissionsPolicy (name)']=='Selective'
    ].index
secondary.loc[
    secondary.index.isin(selective_urns),
    'Number of pupils at selective schools'
    ] = (
        secondary['Number of non-disadvantaged pupils'] +
        secondary['Number of disadvantaged pupils']
        )
secondary.loc[
    ~secondary.index.isin(selective_urns),
    'Number of pupils at selective schools'
    ] = 0

sec_grouped = secondary.groupby(
    ['Governance', 'Governance organisation']
    ).agg(
        {
            'Number of disadvantaged pupils': 'sum',
            'Number of non-disadvantaged pupils': 'sum',
            'Progress 8 total non-disadvantaged': 'sum',
            'Progress 8 total disadvantaged': 'sum',
            'Number of pupils at selective schools': 'sum',
            }
        )
sec_grouped['Number of pupils'] = (
    sec_grouped['Number of disadvantaged pupils'] +
    sec_grouped['Number of non-disadvantaged pupils']
    )
sec_grouped['% disadvantaged'] = (
    sec_grouped['Number of disadvantaged pupils'] /
    sec_grouped['Number of pupils']
    )
sec_grouped['Progress 8 - non-disadvantaged'] = (
    sec_grouped['Progress 8 total non-disadvantaged'] /
    sec_grouped['Number of non-disadvantaged pupils']
    )
sec_grouped['Progress 8 - disadvantaged'] = (
    sec_grouped['Progress 8 total disadvantaged'] /
    sec_grouped['Number of disadvantaged pupils']
    )
sec_grouped['Progress 8 - all'] = (
    (
        sec_grouped['Progress 8 total disadvantaged'] +
        sec_grouped['Progress 8 total non-disadvantaged']
        ) /
    sec_grouped['Number of pupils']
    )
sec_grouped['Selective schools %'] = (
    sec_grouped['Number of pupils at selective schools'] /
    sec_grouped['Number of pupils']
    )

sec_grouped_out = sec_grouped.reset_index()
groups = {}
sec_group_summary = {
        'Governance': [],
        'Percentile': [],
        'Progress 8 - all': [],
        'Disadvantaged %\nAll pupils group': [],
        'Selective schools %\nAll pupils group': [],
        'Average number of pupils sitting exam\nAll pupils group': [],
        'Progress 8 - disadvantaged': [],
        'Disadvantaged %\nDisadvantaged pupils group': [],
        'Selective schools %\nDisadvantaged pupils group': [],
        'Average number of pupils sitting exam\nDisadvantaged pupils group': [],
        }
for group in ['Maintained', 'Single-academy trust', 'Multi-academy trust']:
    groups[group] = {
        'df': sec_grouped_out.loc[sec_grouped_out.Governance==group].copy()
        }
    groups[group]['df']
    for percentile in [10, 25, 50, 75, 90]:
        sec_group_summary['Percentile'].append(percentile)
        sec_group_summary['Governance'].append(group)
        for pupil_group in ['All', 'Disadvantaged']:
            sort = f'Progress 8 - {pupil_group.lower()}'
            groups[group]['df'].sort_values(
                sort,
                ascending=False,
                inplace=True
                )
            sec_group_summary[sort].append(
                groups[group]['df'][sort].quantile(percentile/100)
                )
            if percentile < 50:
                percentile_group = groups[group]['df'].tail(
                    round(groups[group]['df'].shape[0]*percentile/100)
                    )
            elif percentile == 50:
                percentile_group = groups[group]['df']
            else:
                percentile_group = groups[group]['df'].head(
                    round(groups[group]['df'].shape[0]*(1-percentile)/100)
                    )
            sec_group_summary[
                f'Disadvantaged %\n{pupil_group} pupils group'
                ].append(
                    percentile_group['Number of disadvantaged pupils'].sum() /
                    percentile_group['Number of pupils'].sum(),
                    )
            sec_group_summary[
                f'Selective schools %\n{pupil_group} pupils group'
                ].append(
                    percentile_group['Number of pupils at selective schools'].sum() /
                    percentile_group['Number of pupils'].sum(),
                    )
            sec_group_summary[f'Average number of pupils sitting exam\n\
{pupil_group} pupils group'].append(
                round(percentile_group['Number of pupils'].mean())
                )
                              
writer = ExcelWriter(
    'output/Secondary.xlsx',
    engine='xlsxwriter',
    )
summary = DataFrame(sec_group_summary).set_index(['Percentile', 'Governance'])\
    .unstack().transpose()
summary.to_excel(
    writer,
    'Summary'
    )
for group in ['Maintained', 'Single-academy trust', 'Multi-academy trust']:
    groups[group]['df'].to_excel(
        writer,
        group,
        index=False,
        )
writer.close()


ks2 = read_csv(
    'data/exam_accounts_workforce/2018-2019/england_ks2final.csv',
    usecols = [
         'URN',
         'TFSM6CLA1A',
         'TNotFSM6CLA1A',
         'PTRWM_EXP_FSM6CLA1A',
         'PTRWM_EXP_NotFSM6CLA1A',
         ],
    na_values = ['NA', 'NP', 'SUPP', 'LOWCOV', 'NE', ' '],
    )
ks2.columns = [
    'URN',
    'Number of disadvantaged pupils',
    'Number of non-disadvantaged pupils',
    '% expected level - disadvantaged',
    '% expected level - non-disadvantaged',
    ]
for col in [
        '% expected level - disadvantaged',
        '% expected level - non-disadvantaged',
        ]:
    ks2[col] = ks2[col].str.slice(0,-1)
    ks2[col] = to_numeric(ks2[col], errors='coerce')
ks2.dropna(how='any', inplace=True)
ks2.set_index('URN', inplace=True)
ks2['Number expected level - disadvantaged'] = round(
    ks2['Number of disadvantaged pupils'] *
    ks2['% expected level - disadvantaged'] / 100
    )
ks2['Number expected level - non-disadvantaged'] = round(
    ks2['Number of non-disadvantaged pupils'] *
    ks2['% expected level - non-disadvantaged'] / 100
    )

primary = mainstream_at_date.merge(
    ks2,
    how='inner',
    left_index=True,
    right_index=True
    )
prim_grouped = primary.groupby(
    ['Governance', 'Governance organisation']
    ).agg(
        {
            'Number of disadvantaged pupils': 'sum',
            'Number of non-disadvantaged pupils': 'sum',
            'Number expected level - disadvantaged': 'sum',
            'Number expected level - non-disadvantaged': 'sum'
            }
        )
prim_grouped['Number of pupils'] = (
    prim_grouped['Number of disadvantaged pupils'] +
    prim_grouped['Number of non-disadvantaged pupils']
    )
prim_grouped['Expected level - all'] = (
    (
        prim_grouped['Number expected level - disadvantaged'] +
        prim_grouped['Number expected level - non-disadvantaged']
        ) /
    prim_grouped['Number of pupils']
    )
prim_grouped['Expected level - disadvantaged'] = (
    prim_grouped['Number expected level - disadvantaged'] /
    prim_grouped['Number of disadvantaged pupils']
    )

prim_grouped['Attainment gap - difference % expected level'] = (
    prim_grouped['Expected level - all'] -
    prim_grouped['Expected level - disadvantaged'] 
    )

prim_grouped_out = prim_grouped.reset_index()
groups = {}
prim_group_summary = {
    'Governance': [],
    'Percentile': [],
    'Expected level - all': [],
    'Disadvantaged %\nAll pupils group': [],
    'Average number of pupils sitting exam\nAll pupils group': [],
    'Expected level - disadvantaged': [],
    'Disadvantaged %\nDisadvantaged pupils group': [],
    'Average number of pupils sitting exam\nDisadvantaged pupils group': [],
    }
for group in ['Maintained', 'Single-academy trust', 'Multi-academy trust']:
    groups[group] = {
        'df': prim_grouped_out.loc[prim_grouped_out.Governance==group].copy()
        }
    groups[group]['df']
    for percentile in [10, 25, 50, 75, 90]:
        prim_group_summary['Percentile'].append(percentile)
        prim_group_summary['Governance'].append(group)
        for pupil_group in ['All', 'Disadvantaged']:
            sort = f'Expected level - {pupil_group.lower()}'
            groups[group]['df'].sort_values(
                sort,
                ascending=False,
                inplace=True
                )
            prim_group_summary[sort].append(
                groups[group]['df'][sort].quantile(percentile/100)
                )
            if percentile < 50:
                percentile_group = groups[group]['df'].tail(
                    round(groups[group]['df'].shape[0]*percentile/100)
                    )
            elif percentile == 50:
                percentile_group = groups[group]['df']
            else:
                percentile_group = groups[group]['df'].head(
                    round(groups[group]['df'].shape[0]*(1-percentile)/100)
                    )
            prim_group_summary[
                f'Disadvantaged %\n{pupil_group} pupils group'
                ].append(
                    percentile_group['Number of disadvantaged pupils'].sum() /
                    percentile_group['Number of pupils'].sum(),
                    )
            prim_group_summary[f'Average number of pupils sitting exam\n\
{pupil_group} pupils group'].append(
                round(percentile_group['Number of pupils'].mean())
                )

writer = ExcelWriter(
    'output/Primary.xlsx',
    engine='xlsxwriter',
    )
summary = DataFrame(prim_group_summary)\
    .set_index(['Percentile', 'Governance']).unstack().transpose()
summary.to_excel(
    writer,
    'Summary',
    )
for group in ['Maintained', 'Single-academy trust', 'Multi-academy trust']:
    groups[group]['df'].to_excel(
        writer,
        group,
        index=False,
        )
writer.close()
