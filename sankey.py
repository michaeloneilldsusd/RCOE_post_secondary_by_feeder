import pandas as pd
import plotly.graph_objects as go



file_path = ('output.csv')
df = pd.read_csv(file_path, low_memory=False)
df["Middle School"] = "Other CA School"
# df.loc[df['Gr 8 School'] == "No California Enrollment", 'Middle School'] = "No California Enrollment"
preferred_districts = ["No California Enrollment", 'Perris Union High', 'Romoland Elementary', 'Nuview Union', 'Menifee Union', 'Perris Elementary']
for pref in preferred_districts:
    df.loc[df['Gr 8 District'] == pref, 'Middle School'] = df['Gr 8 School']

# Step 1: Combine all unique labels
labels = pd.unique(df[['Middle School', 'SchoolName', 'College_Name']].values.ravel()).tolist()

# Step 2: Build source-target pairings and their counts
def get_links(source_col, target_col):
    link_df = df.groupby([source_col, target_col]).size().reset_index(name='count')
    link_df['source'] = link_df[source_col].apply(lambda x: labels.index(x))
    link_df['target'] = link_df[target_col].apply(lambda x: labels.index(x))
    return link_df[['source', 'target', 'count']]

links_ms_hs = get_links('Middle School', 'SchoolName')
links_hs_college = get_links('SchoolName', 'College_Name')

# Step 3: Concatenate both link levels
all_links = pd.concat([links_ms_hs, links_hs_college])

# Step 4: Build Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=labels
    ),
    link=dict(
        source=all_links['source'],
        target=all_links['target'],
        value=all_links['count']
    )
)])

fig.update_layout(title_text="Middle School → High School → College Flow", font_size=10)
fig.show()
