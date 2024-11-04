from elasticsearch import Elasticsearch
import pandas as pd


es = Elasticsearch([{"host": "localhost", "port": 8989}])


# Function to create a collection
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Created collection: {p_collection_name}")
    else:
        print(f"Collection {p_collection_name} already exists.")


# Function to index data, excluding a specified column
def indexData(p_collection_name, p_exclude_column):
    df = pd.read_csv("employee_data.csv")  # Load dataset
    if p_exclude_column in df.columns:
        df = df.drop(columns=[p_exclude_column])  # Exclude the specified column
    for i, row in df.iterrows():
        doc = row.to_dict()
        res = es.index(index=p_collection_name, id=row["EmployeeID"], document=doc)
        print(f"Indexed document {i+1} into {p_collection_name}")


# Function to search by a specific column value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {"query": {"match": {p_column_name: p_column_value}}}
    res = es.search(index=p_collection_name, body=query)
    print(
        f"Search results for {p_column_name}={p_column_value} in {p_collection_name}:"
    )
    for hit in res["hits"]["hits"]:
        print(hit["_source"])


# Function to get employee count in a collection
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)["count"]
    print(f"Employee count in {p_collection_name}: {count}")
    return count


# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    try:
        es.delete(index=p_collection_name, id=p_employee_id)
        print(f"Deleted employee with ID {p_employee_id} from {p_collection_name}")
    except Exception as e:
        print(f"Error deleting employee: {e}")


# Function to get department facet (count by department)
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {"departments": {"terms": {"field": "Department.keyword"}}},
    }
    res = es.search(index=p_collection_name, body=query)
    print(f"Department facet counts for {p_collection_name}:")
    for bucket in res["aggregations"]["departments"]["buckets"]:
        print(f"{bucket['key']}: {bucket['doc_count']}")


# Define collection variables
v_nameCollection = "Hash_Logeshwaran"  # Replace 'YourName' with your actual name
v_phoneCollection = "Hash_4964"  # Replace '1234' with your phone's last four digits

# Function executions with screenshots suggested after each step
createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, "Department")
indexData(v_phoneCollection, "Gender")
getEmpCount(v_nameCollection)
delEmpById(v_nameCollection, "E02003")  # Ensure 'E02003' exists in the dataset
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection, "Department", "IT")
searchByColumn(v_nameCollection, "Gender", "Male")
searchByColumn(v_phoneCollection, "Department", "IT")
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
