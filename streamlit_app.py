import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Country Information Dashboard", layout="wide")

@st.cache_data
def load_data():
    return pd.read_csv('data/country_info.csv')

def main():
    st.title("🌍 Country Information Dashboard")
    st.markdown("Explore country data including regions, economic groups, and international affiliations")
    
    df = load_data()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Countries", len(df))
    with col2:
        st.metric("Regions", df['region7'].nunique())
    with col3:
        st.metric("G7 Members", df['group_g7'].sum())
    with col4:
        st.metric("EU Members", df['group_european_union'].sum())
    
    st.header("🔍 Data Explorer")
    
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "🌎 Regional Analysis", "🏛️ Group Memberships"])
    
    with tab1:
        st.subheader("Countries by Region")
        region_counts = df['region7'].value_counts()
        fig1 = px.bar(x=region_counts.values, y=region_counts.index, orientation='h',
                     labels={'x': 'Number of Countries', 'y': 'Region'},
                     title="Distribution of Countries by Region")
        st.plotly_chart(fig1, use_container_width=True)
        
        st.subheader("Economic Groups Distribution")
        econ_counts = df['econ_group'].value_counts()
        fig2 = px.pie(values=econ_counts.values, names=econ_counts.index,
                     title="Countries by Economic Development Level")
        st.plotly_chart(fig2, use_container_width=True)
        
        st.subheader("Search Countries")
        search_term = st.text_input("Search for a country:")
        if search_term:
            filtered_df = df[df['country'].str.contains(search_term, case=False, na=False)]
            st.dataframe(filtered_df, use_container_width=True)
    
    with tab2:
        st.subheader("Regional Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_region = st.selectbox("Select Region:", df['region7'].unique())
            region_df = df[df['region7'] == selected_region]
            
            st.write(f"**{selected_region}** has {len(region_df)} countries:")
            st.dataframe(region_df[['country', 'econ_group']], use_container_width=True)
        
        with col2:
            econ_by_region = df.groupby(['region7', 'econ_group']).size().reset_index(name='count')
            fig3 = px.bar(econ_by_region, x='region7', y='count', color='econ_group',
                         title="Economic Groups by Region",
                         labels={'region7': 'Region', 'count': 'Number of Countries'})
            fig3.update_xaxis(tickangle=45)
            st.plotly_chart(fig3, use_container_width=True)
    
    with tab3:
        st.subheader("International Group Memberships")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**G7 Members:**")
            g7_countries = df[df['group_g7'] == True]['country'].tolist()
            for country in g7_countries:
                st.write(f"• {country}")
        
        with col2:
            st.write("**EU Members:**")
            eu_countries = df[df['group_european_union'] == True]['country'].tolist()
            for country in eu_countries[:10]:
                st.write(f"• {country}")
            if len(eu_countries) > 10:
                st.write(f"... and {len(eu_countries) - 10} more")
        
        with col3:
            st.write("**ASEAN5 Members:**")
            asean_countries = df[df['group_asean5'] == True]['country'].tolist()
            for country in asean_countries:
                st.write(f"• {country}")
        
        st.subheader("Membership Overlap Analysis")
        membership_data = []
        for _, row in df.iterrows():
            memberships = []
            if row['group_g7']:
                memberships.append('G7')
            if row['group_european_union']:
                memberships.append('EU')
            if row['group_asean5']:
                memberships.append('ASEAN5')
            
            membership_data.append({
                'Country': row['country'],
                'Memberships': ', '.join(memberships) if memberships else 'None',
                'Count': len(memberships)
            })
        
        membership_df = pd.DataFrame(membership_data)
        fig4 = px.histogram(membership_df, x='Count', 
                           title="Distribution of Group Memberships per Country",
                           labels={'Count': 'Number of Group Memberships', 'count': 'Number of Countries'})
        st.plotly_chart(fig4, use_container_width=True)
    
    st.header("📋 Full Dataset")
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()