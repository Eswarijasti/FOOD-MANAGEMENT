import streamlit as st
import pandas as pd
import mysql.connector

# IMPORT QUERIES (FIX YOU WANTED)
from analysis_details import *

# =========================================================
# DB CONNECTION
# =========================================================
def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Eswari@99",
        database="FOOD_MANGEMENT"
    )

def run_query(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def execute_query(query, values=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, values)
    conn.commit()
    conn.close()

# =========================================================
# UI CONFIG
# =========================================================
st.set_page_config(layout="wide")
st.title("🍱 Food Wastage Management System")

page = st.sidebar.radio("Navigation", [
    "Project Introduction",
    "Data Management",
    "SQL Analysis"
])

# =========================================================
# INTRO
# =========================================================
if page == "Project Introduction":

    st.header("📌 Food Mangement System")

    st.markdown("""
### 🌍 Overview
This system connects providers and receivers.

### ⚙️ Features
- CRUD operations  
- MySQL integration  
- SQL analytics  
- Interactive dashboard  

### 🎯 Goal
Reduce food waste using data-driven insights.
""")

# =========================================================
# DATA MANAGEMENT
# =========================================================
elif page == "Data Management":

    st.header("📂 Data Management")

    table = st.selectbox("Select Table", [
        "providers",
        "receivers",
        "food_listings",
        "claims"
    ])

    id_col = {
        "providers": "provider_id",
        "receivers": "receiver_id",
        "food_listings": "food_id",
        "claims": "claim_id"
    }[table]

    # =========================================================
    # VIEW (COLLAPSIBLE FIX)
    # =========================================================
    with st.expander("📊 View Data"):
        df = run_query(f"SELECT * FROM {table}")
        st.dataframe(df)

    # =========================================================
    # ADD (COLLAPSIBLE FIX)
    # =========================================================
    with st.expander("➕ Add Data"):

        if table == "providers":
            name = st.text_input("Name")
            type_ = st.text_input("Type")
            address = st.text_input("Address")
            city = st.text_input("City")
            contact = st.text_input("Contact")

            if st.button("Add Provider"):
                execute_query("""
                    INSERT INTO providers (name, type, address, city, contact)
                    VALUES (%s,%s,%s,%s,%s)
                """, (name, type_, address, city, contact))
                st.success("Added")

        elif table == "receivers":
            name = st.text_input("Name")
            type_ = st.text_input("Type")
            city = st.text_input("City")
            contact = st.text_input("Contact")

            if st.button("Add Receiver"):
                execute_query("""
                    INSERT INTO receivers (name, type, city, contact)
                    VALUES (%s,%s,%s,%s)
                """, (name, type_, city, contact))
                st.success("Added")

        elif table == "food_listings":
            food_name = st.text_input("Food Name")
            quantity = st.number_input("Quantity", min_value=0.0, step=1.0)
            provider_id = st.number_input("Provider ID", min_value=0, step=1)
            location = st.text_input("Location")

            if st.button("Add Food"):
                execute_query("""
                    INSERT INTO food_listings (food_name, quantity, provider_id, location)
                    VALUES (%s,%s,%s,%s)
                """, (food_name, quantity, provider_id, location))
                st.success("Added")

        elif table == "claims":
            food_id = st.number_input("Food ID", min_value=0, step=1)
            receiver_id = st.number_input("Receiver ID", min_value=0, step=1)
            status = st.selectbox("Status", ["Pending", "Completed"])

            if st.button("Add Claim"):
                execute_query("""
                    INSERT INTO claims (food_id, receiver_id, status)
                    VALUES (%s,%s,%s)
                """, (food_id, receiver_id, status))
                st.success("Added")

    # =========================================================
    # UPDATE (FULL ROW FIX)
    # =========================================================
    with st.expander("✏️ Update Data"):

        record_id = st.number_input(f"Enter {id_col}", min_value=1, step=1)

        df = run_query(f"SELECT * FROM {table} WHERE {id_col}={record_id}")

        if not df.empty:
            row = df.iloc[0]

            if table == "providers":
                name = st.text_input("Name", row["name"])
                type_ = st.text_input("Type", row["type"])
                address = st.text_input("Address", row["address"])
                city = st.text_input("City", row["city"])
                contact = st.text_input("Contact", row["contact"])

                if st.button("Update Provider"):
                    execute_query("""
                        UPDATE providers
                        SET name=%s, type=%s, address=%s, city=%s, contact=%s
                        WHERE provider_id=%s
                    """, (name, type_, address, city, contact, record_id))
                    st.success("Updated")

            elif table == "receivers":
                name = st.text_input("Name", row["name"])
                type_ = st.text_input("Type", row["type"])
                city = st.text_input("City", row["city"])
                contact = st.text_input("Contact", row["contact"])

                if st.button("Update Receiver"):
                    execute_query("""
                        UPDATE receivers
                        SET name=%s, type=%s, city=%s, contact=%s
                        WHERE receiver_id=%s
                    """, (name, type_, city, contact, record_id))
                    st.success("Updated")

            elif table == "food_listings":
                food_name = st.text_input("Food Name", row["food_name"])
                quantity = st.number_input("Quantity", value=float(row["quantity"]))
                location = st.text_input("Location", row["location"])

                if st.button("Update Food"):
                    execute_query("""
                        UPDATE food_listings
                        SET food_name=%s, quantity=%s, location=%s
                        WHERE food_id=%s
                    """, (food_name, quantity, location, record_id))
                    st.success("Updated")

            elif table == "claims":
                status = st.selectbox("Status", ["Pending", "Completed"])

                if st.button("Update Claim"):
                    execute_query("""
                        UPDATE claims
                        SET status=%s
                        WHERE claim_id=%s
                    """, (status, record_id))
                    st.success("Updated")

    # =========================================================
    # DELETE (FIX ZERO ISSUE + EXPANDER)
    # =========================================================
    with st.expander("🗑 Delete Data"):

        delete_id = st.number_input(
            f"Enter {id_col} to delete",
            min_value=1,
            step=1
        )

        if st.button("Delete Record"):
            execute_query(f"""
                DELETE FROM {table}
                WHERE {id_col}=%s
            """, (int(delete_id),))
            st.warning("Deleted Successfully")

# =========================================================
# SQL ANALYSIS (USING IMPORTED QUERIES)
# =========================================================
elif page == "SQL Analysis":

    st.header("📊 Analytics Dashboard")

    query_map = {
        "Providers & Receivers per City": q1,
        "Top Provider Type": q2,
        "Provider Contacts by City": q3,
        "Top Receivers": q4,
        "Total Food": q5,
        "City Highest Listings": q6,
        "Common Food Types": q7,
        "Claims per Food": q8,
        "Top Provider by Claims": q9,
        "Claim Status %": q10,
        "Avg Quantity per Receiver": q11,
        "Most Claimed Meal": q12,
        "Total Food per Provider": q13
    }

    selected = st.selectbox("Select Query", list(query_map.keys()))

    df = run_query(query_map[selected])
    st.dataframe(df)

    if not df.empty:
        st.bar_chart(df.set_index(df.columns[0]))