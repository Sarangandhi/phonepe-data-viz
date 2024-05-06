import numpy as np
import pandas as pd
from mysql.connector import connect
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# -- importing all the Phonepe pulse data
config = {
    'user': 'your_user_name',
    'password': 'your_password',
    'host': 'your_hostname',
    'database': 'your_database_name',
    'port': '3306'  # Default MySQL port
}


def connect_to_db(config):
    # Create the MySQL connection
    conn = connect(
        host=config["host"],
        user=config["user"],
        passwd=config["password"],
        database=config["database"]
    )
    return conn


def fetch_top_country_data(connection, data_type, year=None, quarter=None):
    # Read data from MySQL table into a DataFrame
    if data_type == "Insurance":
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_insurance_country_data
        WHERE year = '{year}' AND quarter = '{quarter}'"""
    else:
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_transaction_country_data 
        WHERE year = '{year}' AND quarter = '{quarter}'"""
    country_df = pd.read_sql_query(sql, connection)
    return country_df


def fetch_top_state_data(connection, data_type, year=None, quarter=None):
    if data_type == "Insurance":
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_insurance_state_data
        WHERE year = '{year}' AND quarter = '{quarter}'"""
    else:
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_transaction_state_data
        WHERE year = '{year}' AND quarter = '{quarter}'"""
    state_data = pd.read_sql_query(sql, connection)
    return state_data


def fetch_aggregated_country_data(connection, data_type, year=None, quarter=None, full=None):
    if full:
        if data_type == "Insurance":
            sql = f"SELECT * FROM phonepe_data_viz.aggregated_insurance_country_data;"
        else:
            sql = f"SELECT * FROM phonepe_data_viz.aggregated_transaction_country_data;"
    else:
        if data_type == "Insurance":
            sql = f"""
            SELECT * FROM phonepe_data_viz.aggregated_insurance_country_data
            WHERE year = '{year}' AND quarter = '{quarter}'"""
        else:
            sql = f"""
            SELECT * FROM phonepe_data_viz.aggregated_transaction_country_data
            WHERE year = '{year}' AND quarter = '{quarter}'"""
    country_data = pd.read_sql_query(sql, connection)
    country_data["year"] = country_data["year"].astype(int)
    return country_data


def fetch_aggregated_state_data(connection, data_type, year=None, quarter=None):
    if data_type == "Insurance":
        sql = f"""
        SELECT * FROM phonepe_data_viz.aggregated_insurance_state_data
         WHERE year = '{year}' AND quarter = '{quarter}'"""
    else:
        sql = f"""
        SELECT * FROM phonepe_data_viz.aggregated_transaction_state_data
        WHERE year = '{year}' AND quarter = '{quarter}'"""
    state_data = pd.read_sql_query(sql, connection)
    return state_data


def get_total_average_amount_and_count(conn, data_type):
    if data_type == "Insurance":
        sql = """
        SELECT sum(payment_instruments_amount) as total_amount, 
        avg(payment_instruments_amount) as avg_amount,
        sum(payment_instruments_count) as total_count
        FROM phonepe_data_viz.aggregated_transaction_country_data;
        """
    else:
        sql = """
        SELECT sum(payment_instruments_amount) as total_amount,
        avg(payment_instruments_amount) as avg_amount,
        sum(payment_instruments_count) as total_count 
        FROM phonepe_data_viz.aggregated_insurance_country_data;
        """

    df = pd.read_sql(sql, conn)
    return df


def fetch_map_lat_lng_data(connection, year, quarter):
    sql = f"""
    SELECT state, lat, lng, metric FROM phonepe_data_viz.map_insurance_country_data WHERE (state, id) IN (
        SELECT state, MIN(id) FROM phonepe_data_viz.map_insurance_country_data 
        WHERE year = '{year}' AND quarter = '{quarter}' GROUP BY state);
    """

    def capitalize_state(state):
        # Capitalize each word
        return state.title()
    state_data = pd.read_sql_query(sql, connection)
    state_data["state"] = state_data["state"].apply(capitalize_state)
    state_data["state"] = state_data["state"].replace(
        {
            "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu",
            "Andaman & Nicobar Islands": "Andaman & Nicobar"
        }
    )
    state_data.sort_values(by='state', inplace=True)
    return state_data


def fetch_aggregated_user_data(connection, year, quarter):
    country_sql = f"""
    SELECT * FROM phonepe_data_viz.aggregated_user_and_device_country_data
    where year = '{year}' and quarter = '{quarter}';"""
    state_sql = f"""
    SELECT * FROM phonepe_data_viz.aggregated_user_and_device_state_data
    where year = '{year}' and quarter = '{quarter}';"""
    country_df = pd.read_sql(country_sql, connection)
    state_df = pd.read_sql(state_sql, connection)
    country_df[['count', 'percentage']] = country_df[['count', 'percentage']].astype(np.float32)
    state_df[['count', 'percentage']] = state_df[['count', 'percentage']].astype(np.float32)
    return country_df, state_df


def extract_district_transaction_data(connection, data_type, state, year, quarter):

    # state = state.replace("-", " ")
    state = state.title()
    if data_type == "Transaction":
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_transaction_state_data
        where state = '{state}' and year = '{year}' and quarter = '{quarter}';"""
    else:
        sql = f"""
        SELECT * FROM phonepe_data_viz.top_insurance_state_data where
        state = '{state}' and year = '{year}' and quarter = '{quarter}';"""
    df = pd.read_sql(sql, con=connection)
    df[['amount', 'count']] = df[['amount', 'count']].astype(float)
    grouped_df = (df.groupby(['state', 'district', 'year', 'quarter']).agg({'amount': 'sum', 'count': 'sum'}).
                  reset_index())
    total_amount = sum(grouped_df['amount'].tolist())
    average_amount = sum(grouped_df['count'].tolist())
    grouped_df.rename(columns={'amount': 'Total Amount Transactions', 'count': 'Total Count of Transactions'},
                      inplace=True)

    return grouped_df, total_amount, average_amount


def create_bar_chart(data, x_axis, y_axis, title, color=None):
    data[y_axis] = data[y_axis].astype(float)
    if not color:
        grouped_df = data.groupby([x_axis]).agg({y_axis: "sum"}).reset_index()
        grouped_df = grouped_df.nlargest(n=5, columns=[y_axis])
        fig = px.bar(
            grouped_df, x=x_axis, y=y_axis, title=f"<b>{title}</b>",
            color_discrete_sequence=["#008388"] * len(x_axis)
        )
    else:
        grouped_df = data.groupby([x_axis, color]).agg({y_axis: "mean"}).reset_index()
        # grouped_df = grouped_df.nlargest(n=5, columns=[y_axis])
        fig = px.bar(
            grouped_df, x=x_axis, y=y_axis, title=f"<b>{title}</b>",
            color=color
        )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def create_pie_chart(data, x_axis, y_axis, title):
    data[y_axis] = data[y_axis].astype(float)
    grouped_df = data.groupby([x_axis]).agg({y_axis: "sum"}).reset_index()
    fig = px.pie(
        grouped_df,
        names=x_axis,
        values=y_axis,
        color_discrete_sequence=["#008388"],
        title=f"<b>{title}<b>",
        width=500,
        height=500
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def create_multiple_line_chart(data, x_axis, y_axis_1, color, title):
    data[y_axis_1] = data[y_axis_1].astype(float)
    grouped_df = data.groupby([x_axis, color]).agg({y_axis_1: "sum"}).reset_index()
    fig = px.line(
        grouped_df,
        x=x_axis,
        y=y_axis_1,
        color=color,
        title=f"<b>{title}</b>"
    )
    # fig.update_traces(mode='lines+markers')
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
    )
    # Set x-axis tick values to integer years
    fig.update_xaxes(tickvals=grouped_df[x_axis])
    return fig


def plot_map(map_lat_lon_data, title, selected_state=None):
    fig = px.choropleth(
        map_lat_lon_data[["state", "metric"]],
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='state',
        color='state',
        color_discrete_sequence=["#008388"],
        color_discrete_map={selected_state: "red"},
        title=f"<b>{title}</b>"
    )
    fig.update_geos(fitbounds="locations", visible=False)

    return fig


def state_cards(data):
    # Calculate summary statistics
    total_count = data['Total Count of Transactions'].sum()
    total_amount = data['Total Amount Transactions'].sum()
    average_count = data['Total Count of Transactions'].mean()
    average_amount = data['Total Amount Transactions'].mean()

    # Create cards
    card1 = go.Indicator(
        mode="number",
        value=total_count,
        title={"text": "Total Count"},
        domain={'x': [0, 0.5], 'y': [0, 0.5]},
        number={'suffix': " ", 'font': {'color': "#38E54D"}}
    )

    card2 = go.Indicator(
        mode="number",
        value=total_amount,
        title={"text": "Total Amount"},
        domain={'x': [0.5, 1], 'y': [0, 0.5]},
        number={'suffix': " ", 'font': {'color': "#38E54D"}}
    )

    card3 = go.Indicator(
        mode="number",
        value=average_count,
        title={"text": "Average Count"},
        domain={'x': [0, 0.5], 'y': [0.5, 1]},
        number={'suffix': " ", 'font': {'color': "#38E54D"}}
    )

    card4 = go.Indicator(
        mode="number",
        value=average_amount,
        title={"text": "Average Amount"},
        domain={'x': [0.5, 1], 'y': [0.5, 1]},
        number={'suffix': " ", 'font': {'color': "#38E54D"}}
    )

    # Create layout
    layout = go.Layout(
        title="Summary Statistics",
        grid={'rows': 2, 'columns': 2, 'pattern': "independent"}
    )

    # Create figure
    fig = go.Figure(data=[card1, card2, card3, card4], layout=layout)
    return fig


# Streamlit App
def main():
    # Connect to the database
    connection = connect_to_db(config)
    # print(connection)
    # Fetch data from MySQL
    st.set_page_config(layout='wide')

    st.markdown(
        """
        <style>
            div[data-testid="column"]:nth-of-type(1)
            {
                # border:1px solid red;
            } 

            div[data-testid="column"]:nth-of-type(2)
            {
                # border:1px solid blue;
                text-align: end;
            } 
        </style>
        """, unsafe_allow_html=True
    )
    data_type = st.sidebar.selectbox("select Type Of Data", ["Insurance", "Transaction"], index=1)
    if data_type == "Insurance":
        st.markdown(f"<h1 style='text-align: center; color: white;'>{data_type} Dashboard</h1>", unsafe_allow_html=True)
    else:
        st.markdown(f"<h1 style='text-align: center; color: white;'>{data_type} Dashboard</h1>", unsafe_allow_html=True)
    st.title("Country")
    year = st.sidebar.selectbox("Select Year", [2020, 2021, 2022, 2023], index=1)
    quarter = st.sidebar.selectbox("Select Quarter", ["Q1", "Q2", "Q3", "Q4"], index=0)

    map_lat_lon_data = fetch_map_lat_lng_data(connection, year, quarter)

    selected_state = st.sidebar.selectbox("Select State", map_lat_lon_data["state"].tolist(), index=1)

    top_country_data = fetch_top_country_data(connection, data_type, year, quarter)

    aggregated_country_data = fetch_aggregated_country_data(connection, data_type, year, quarter)
    aggregated_state_data = fetch_aggregated_state_data(connection, data_type, year, quarter)

    aggregated_country_data['payment_instruments_amount'] = (aggregated_country_data['payment_instruments_amount'].
                                                             astype(np.float64))
    total_amount = aggregated_country_data['payment_instruments_amount'].sum()
    avg_amount = aggregated_country_data['payment_instruments_amount'].mean()

    user_country_data, user_state_data = fetch_aggregated_user_data(connection, year, quarter)
    left, middle, right = st.columns(3)
    with left:
        st.subheader(f"Total {data_type} Amount:")
        st.subheader(f"₹ {int(total_amount):,}")
        bar_chart = create_bar_chart(top_country_data, "state", "amount", title="Top 5 State On Transaction")
        st.plotly_chart(bar_chart)

        full_data_for_line = fetch_aggregated_country_data(connection, data_type, full=True)
        line_chart = create_multiple_line_chart(
            full_data_for_line, "year", "payment_instruments_count", color="transaction_name",
            title="Total Count Of Transaction By Category"
        )
        st.plotly_chart(line_chart)
        # Bar Chart - Count of registered users for each brand
        bar_fig = px.bar(data_frame=user_country_data, x='brand', y='count',
                         title='Count of Registered Users by Brand', orientation='v')
        st.plotly_chart(bar_fig)

    with right:
        st.subheader(f"Average {data_type} Amount:")
        st.subheader(f"₹ {int(avg_amount):,}")
        pie_chart = create_pie_chart(
            aggregated_country_data, "transaction_name", "payment_instruments_amount",
            title="Distribution Of Transaction By Category"
        )
        st.plotly_chart(pie_chart)

        stacked_bar_chart = create_bar_chart(
            aggregated_state_data, "state", "payment_instruments_amount",
            title="Average Transaction Amount By Transaction Type And State", color="transaction_name"
        )
        st.plotly_chart(stacked_bar_chart)
        # Stacked Bar Chart - Count of registered users for each brand, stacked by quarter
        stacked_bar_fig = px.bar(data_frame=user_state_data, x='state', y='count', color='brand',
                                 title='Count of Registered Users by Brand, Stacked by States')
        st.plotly_chart(stacked_bar_fig)
    st.title("State")
    map_india = plot_map(map_lat_lon_data, "States Level Transaction Data", selected_state)
    st.plotly_chart(map_india)

    (extract_district_top_transaction_data,
     total_district_amount, total_district_count) = extract_district_transaction_data(
        connection, data_type, selected_state, year, quarter
    )
    left, right = st.columns(2)
    with left:
        st.subheader(f"Total {data_type} Amount:")
        st.subheader(f"₹ {int(total_district_amount):,}")
        st.markdown("----")

        st.header(f"Top 10 Districts Of {selected_state}")
        st.dataframe(extract_district_top_transaction_data)

        # Create a donut chart
        donut_fig = px.pie(extract_district_top_transaction_data, values='Total Amount Transactions',
                           names='district', hole=0.5, title='Distribution of Amount by District')
        st.plotly_chart(donut_fig)
    with right:
        st.subheader(f"Total {data_type} Count:")
        print()
        st.subheader(f"{int(total_district_count):,}")
        st.markdown("----")
        # Scatter Plot - Count against Amount for each district
        cards = state_cards(extract_district_top_transaction_data)
        st.plotly_chart(cards)

        scatter_fig = px.scatter(extract_district_top_transaction_data, x='Total Count of Transactions',
                                 y='Total Amount Transactions', color='district', title="Count vs Amount by District")
        st.plotly_chart(scatter_fig)

    connection.close()


if __name__ == "__main__":
    main()
