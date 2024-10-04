from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['Hash_Agile_Technology']

def createCollection(p_collection_name):
    db.create_collection(p_collection_name)
    print(f"Collection '{p_collection_name}' created.")

def insertData(p_collection_name):
    collection = db[p_collection_name]
    employee_data = [
        {"ID": "E02001", "Name": "John", "Department": "IT", "Gender": "Male"},
        {"ID": "E02002", "Name": "Jane", "Department": "HR", "Gender": "Female"},
        {"ID": "E02003", "Name": "Sam", "Department": "IT", "Gender": "Male"},
        {"ID": "E02004", "Name": "Anna", "Department": "Finance", "Gender": "Female"}
    ]
    collection.insert_many(employee_data)
    print(f"Data inserted into collection '{p_collection_name}'.")
def indexData(p_collection_name, p_exclude_column):
    collection = db[p_collection_name]
    employee_data = list(collection.find({}))
    
    modified_data = []
    
    for data in employee_data:
        if p_exclude_column in data:
            del data[p_exclude_column] 
        modified_data.append(data)
        
    print(f"Data indexed from collection '{p_collection_name}' excluding '{p_exclude_column}':")
    for item in modified_data:
        print(item)

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    collection = db[p_collection_name]
    results = list(collection.find({p_column_name: p_column_value}))
    
    print(f"Search results in '{p_collection_name}' where '{p_column_name}' is '{p_column_value}':")
    for result in results:
        print(result)

def getEmpCount(p_collection_name):
    collection = db[p_collection_name]
    count = collection.count_documents({})
    print(f"Employee count in '{p_collection_name}': {count}")

def delEmpById(p_collection_name, p_employee_id):
    collection = db[p_collection_name]
    result = collection.delete_one({"ID": p_employee_id})
    if result.deleted_count > 0:
        print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")
    else:
        print(f"Employee with ID '{p_employee_id}' not found in '{p_collection_name}'.")

def getDepFacet(p_collection_name):
    collection = db[p_collection_name]
    pipeline = [
        {"$group": {"_id": "$Department", "count": {"$sum": 1}}}
    ]
    facets = list(collection.aggregate(pipeline))
    
    print(f"Department facet counts in '{p_collection_name}':")
    for facet in facets:
        print(f"Department: {facet['_id']}, Count: {facet['count']}")

if __name__ == "__main__":
    v_nameCollection = 'Hash_AadharshC'  
    v_phoneCollection = 'Hash_7660'      

    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)
    insertData(v_nameCollection)
    insertData(v_phoneCollection)

    getEmpCount(v_nameCollection)
    indexData(v_nameCollection, 'Department')

    indexData(v_phoneCollection, 'Gender')

    delEmpById(v_nameCollection, 'E02003')
    getEmpCount(v_nameCollection)

    searchByColumn(v_nameCollection, 'Department', 'IT')
    searchByColumn(v_nameCollection, 'Gender', 'Male')
    searchByColumn(v_phoneCollection, 'Department', 'IT')

    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)

client.close()
