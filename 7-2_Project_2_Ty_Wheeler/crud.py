from pymongo import MongoClient

class AnimalShelter:
    def __init__(self, username, password, 
                 host="nv-desktop-services.apporto.com", 
                 port=31503):
        # initialize MongoDB connection
        self.client = MongoClient(
            f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
        )
        self.db = self.client["AAC"]
        self.collection = self.db["animals"]

    def create(self, data):
        # Create a new animal record
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        result = self.collection.insert_one(data)
        return result.inserted_id is not None

    def read(self, query):
        # read animal records with given query
        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary")
        return list(self.collection.find(query))

    def update(self, query, new_values):
        # Update existing animal records
        if not isinstance(query, dict) or not isinstance(new_values, dict):
            raise ValueError("Both query and new_values must be dictionaries")
        result = self.collection.update_many(query, {"$set": new_values})
        return result.modified_count

    def delete(self, query):
        # Delete animal records
        if not isinstance(query, dict):
            raise ValueError("Query must be a dictionary")
        result = self.collection.delete_many(query)
        return result.deleted_count

    def get_rescue_query(self, rescue_type):
        # Return query filter for specific rescue types
        queries = {
            'water': {
                'breed': {'$in': [
                    "Labrador Retriever Mix",
                    "Chesapeake Bay Retriever", 
                    "Newfoundland"
                ]},
                'age_upon_outcome_in_weeks': {'$lte': 104},  # 2 years
                'sex_upon_outcome': {'$ne': 'Unknown'}
            },
            'mountain': {
                'breed': {'$in': [
                    "German Shepherd",
                    "Alaskan Malamute",
                    "Old English Sheepdog",
                    "Rottweiler",
                    "Siberian Husky"
                ]},
                'age_upon_outcome_in_weeks': {'$lte': 156},  # 3 years
                'sex_upon_outcome': {'$ne': 'Unknown'}
            },
            'disaster': {
                'breed': {'$in': [
                    "Doberman Pinscher",
                    "German Shepherd",
                    "Golden Retriever",
                    "Bloodhound",
                    "Rottweiler"
                ]},
                'age_upon_outcome_in_weeks': {'$lte': 208},  # 4 years
                'sex_upon_outcome': {'$ne': 'Unknown'}
            }
        }
        return queries.get(rescue_type, {})