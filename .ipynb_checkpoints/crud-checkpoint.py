import psycopg2

class AnimalShelter:
    def __init__(self):
        # Initialize PostgreSQL connection
        self.conn = psycopg2.connect(
            dbname="aac",
            user="tywheeler",
            password="",
            host="localhost",
            port="5432"
        )
        self.cursor = self.conn.cursor()

    def create(self, data):
        # Insert a new animal record into the dogs table
        query = """
            INSERT INTO dogs 
            (name, breed, age_upon_outcome_weeks, sex_upon_outcome, outcome_type, outcome_subtype,
             rescue_type_id, location_id, animal_type, color, date_of_birth, datetime, monthyear,
             location_lat, location_long)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data.get("name"),
            data.get("breed"),
            data.get("age_upon_outcome_weeks"),
            data.get("sex_upon_outcome"),
            data.get("outcome_type"),
            data.get("outcome_subtype"),
            data.get("rescue_type_id"),
            data.get("location_id"),
            data.get("animal_type"),
            data.get("color"),
            data.get("date_of_birth"),
            data.get("datetime"),
            data.get("monthyear"),
            data.get("location_lat"),
            data.get("location_long")
        )
        self.cursor.execute(query, values)
        self.conn.commit()
        return True

    def read(self, rescue_type=None):
        # Read dog record and filter by rescue type
        query = """
            SELECT dogs.id, dogs.name, dogs.breed, dogs.age_upon_outcome_weeks, dogs.sex_upon_outcome,
                   dogs.animal_type, dogs.color, dogs.date_of_birth, dogs.datetime, dogs.monthyear,
                   dogs.outcome_type, dogs.outcome_subtype, dogs.location_lat, dogs.location_long
            FROM dogs
            LEFT JOIN rescue_types ON dogs.rescue_type_id = rescue_types.id
        """
        if rescue_type:
            query += " WHERE rescue_types.name = %s"
            self.cursor.execute(query, (rescue_type,))
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def update(self, dog_id, new_values):
        set_clause = ", ".join([f"{k} = %s" for k in new_values.keys()])
        values = list(new_values.values()) + [dog_id]
        query = f"UPDATE dogs SET {set_clause} WHERE id = %s"
        self.cursor.execute(query, values)
        self.conn.commit()
        return self.cursor.rowcount

    def delete(self, dog_id):
        self.cursor.execute("DELETE FROM dogs WHERE id = %s", (dog_id,))
        self.conn.commit()
        return self.cursor.rowcount

    def get_rescue_query(self, rescue_type):
        queries = {
            'water': {
                'breed': [
                    "Labrador Retriever Mix",
                    "Chesapeake Bay Retriever",
                    "Newfoundland"
                ],
                'max_age_weeks': 104
            },
            'mountain': {
                'breed': [
                    "German Shepherd",
                    "Alaskan Malamute",
                    "Old English Sheepdog",
                    "Rottweiler",
                    "Siberian Husky"
                ],
                'max_age_weeks': 156
            },
            'disaster': {
                'breed': [
                    "Doberman Pinscher",
                    "German Shepherd",
                    "Golden Retriever",
                    "Bloodhound",
                    "Rottweiler"
                ],
                'max_age_weeks': 208
            }
        }
        return queries.get(rescue_type, {})
