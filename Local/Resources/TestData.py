import pandas as pd

# dummy dfs for test merge
df1 = pd.DataFrame(
    {
        'EmployeeNumber': [
            'efitzgerald@azdot.gov',
            'efitzgerald@azdot.gov',
            'efitzgerald@azdot.gov',
            'efitzgerald@azdot.gov',
            'mhardesty@azdot.gov',
            'mhardesty@azdot.gov',
            'mhardesty@azdot.gov',
            'mbrender@azdot.gov',
            'mbrender@azdot.gov',
            'mbrender@azdot.gov'
            ],
        'ActivityCode': [
            'GEN1356W',
            'RM29',
            'LAW1006EMP',
            'ADORI100',
            'GEN1356W',
            'RM29',
            'LAW1006EMP',
            'GEN1356W',
            'RM29',
            'LAW1006EMP'
            ],
        'CompletionDate': [
            '01/12/2024',
            '01/12/2024',
            '01/12/2024',
            '01/12/2024',
            '01/10/2024',
            '01/10/2024',
            '01/10/2024',
            '01/03/2024',
            '01/03/2024',
            '01/03/2024'
        ],
        'Score': [
            100,
            95,
            90,
            80,
            100,
            100,
            100,
            95,
            85,
            80
        ],
        'DataSource': [
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp',
            'TraCorp'
        ]
    }
)


df2 = pd.DataFrame(
    {
        'EmployeeNumber': [
            'marispe@azdot.gov',
            'marispe@azdot.gov',
            'marispe@azdot.gov',
            'tsteffan@azdot.gov',
            'tsteffan@azdot.gov',
            'tsteffan@azdot.gov',
            'nbeydoun@azdot.gov',
            'nbeydoun@azdot.gov',
            'nbeydoun@azdot.gov',
            'efitzgerald@azdot.gov',
            'efitzgerald@azdot.gov',
            'efitzgerald@azdot.gov',
            'mhardesty@azdot.gov',
            'mhardesty@azdot.gov',
            'mhardesty@azdot.gov',
            'mbrender@azdot.gov',
            'mbrender@azdot.gov',
            'mbrender@azdot.gov'
            ],
        'ActivityCode': [
            'GEN1356W',
            'ADORI100',
            'MAP101',
            'GEN1356W',
            'ADORI100',
            'MAP101',
            'GEN1356W',
            'ADORI100',
            'MAP101',
            'GEN1356W',
            'RM29',
            'LAW1006EMP',
            'GEN1356W',
            'RM29',
            'LAW1006EMP',
            'GEN1356W',
            'RM29',
            'LAW1006EMP'
            ],
        'CompletionDate': [
            '01/02/2024',
            '01/02/2024',
            '01/02/2024',
            '01/04/2024',
            '01/04/2024',
            '01/04/2024',
            '01/05/2024',
            '01/05/2024',
            '01/05/2024',
            '01/12/2024',
            '01/12/2024',
            '01/12/2024',
            '01/10/2024',
            '01/10/2024',
            '01/10/2024',
            '01/03/2024',
            '01/03/2024',
            '01/03/2024'
        ],
        'DataSource': [
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal',
            'SumTotal'
        ]
    }
)


# Merge dfs
df3 = df1.merge(df2, how='outer')

# Remove duplicates
df3 = df3.drop_duplicates(subset=['EmployeeNumber', 'ActivityCode', 'CompletionDate'], keep=False)

print(df3)