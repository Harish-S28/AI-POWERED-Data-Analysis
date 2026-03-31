import mysql.connector
import pandas as pd
from groq import Groq
import matplotlib.pyplot as plt

# 🔑 Groq setup
client = Groq(api_key="Grok_API")

# 🧹 Clean SQL
def clean_sql(sql):
    return sql.replace("```sql", "").replace("```", "").strip()

# 📂 Upload CSV → MySQL
def upload_csv_to_mysql(file_path, table_name):
    df = pd.read_csv(file_path)

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="harish@*)(@))%",
        database="ai_project"
    )
    cursor = conn.cursor()

    # Drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Create table dynamically
    columns = ", ".join([f"{col} TEXT" for col in df.columns])
    cursor.execute(f"CREATE TABLE {table_name} ({columns})")

    # Insert data
    for _, row in df.iterrows():
        values = tuple(str(x) for x in row)
        placeholders = ", ".join(["%s"] * len(values))
        cursor.execute(
            f"INSERT INTO {table_name} VALUES ({placeholders})",
            values
        )

    conn.commit()
    conn.close()

    print("✅ CSV uploaded and table created!")

# 📊 Get columns dynamically
def get_columns():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="harish@*)(@))%",
        database="ai_project"
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("DESCRIBE dataset")
    columns = [col["Field"] for col in cursor.fetchall()]

    conn.close()
    return columns


def get_sample_data():
    cursor.execute("SELECT * FROM dataset LIMIT 5")
    return cursor.fetchall()




# 🤖 Question → SQL
def get_sql_from_ai(question, columns):
    sample_data = get_sample_data()

    prompt = f"""
    Convert the following question into SQL query.

    Table name: dataset
    Columns: {columns}

    Sample data:
    {sample_data}

    Rules:
    - Use exact values from dataset
    - If filtering text, use correct value like 'Married'
    - Avoid assumptions like 'M' or 'F'
    - You can use LIKE if needed

    Only return SQL query. No explanation.

    Question: {question}
    """

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return clean_sql(response.choices[0].message.content)



def plot_result(result):
    if not result:
        print("No data to plot")
        return

    keys = list(result[0].keys())

    # Case 1: Aggregate (like SUM, AVG)
    if len(keys) == 1:
        value = list(result[0].values())[0]
        plt.bar(keys[0], value)
        plt.title("Result")
        plt.show()

    # Case 2: Multiple rows (like category vs value)
    elif len(keys) >= 2:
        x = [str(row[keys[0]]) for row in result]
        y = [float(row[keys[1]]) if str(row[keys[1]]).isdigit() else 0 for row in result]

        plt.bar(x, y)
        plt.xlabel(keys[0])
        plt.ylabel(keys[1])
        plt.xticks(rotation=45)
        plt.title("Data Visualization")
        plt.show()


def generate_insight(result):
    if not result:
        print("No data for insight")
        return

    prompt = f"""
    You are a data analyst.

    Analyze the following data and give short business insights.

    Data:
    {result}

    Give 2-3 simple insights.
    """

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    insight = response.choices[0].message.content

    print("\n📊 Insights:")
    print(insight)



# ================= MAIN =================

# 📂 Step 1: Upload CSV
file_path = input("Enter CSV file path: ")
upload_csv_to_mysql(file_path, "dataset")

# 🗄️ DB connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="harish@*)(@))%",
    database="ai_project"
)

cursor = conn.cursor(dictionary=True)

# 📊 Step 2: Get columns
columns = get_columns()

# 💬 Step 3: Ask question
question = input("Ask your question: ")

# 🤖 Step 4: Generate SQL
sql = get_sql_from_ai(question, columns)

print("\nGenerated SQL:", sql)

try:
    cursor.execute(sql)
    result = cursor.fetchall()

    print("\nResult:")
    for row in result:
        print(row)

    
    plot_result(result)
    generate_insight(result)

except Exception as e:
    print("\nError:", e)


# 🔚 Close
conn.close()
