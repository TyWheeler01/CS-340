import pandas as pd
from crud import AnimalShelter

# Initialize the database connection
db = AnimalShelter()
rescue_rules = db.get_rescue_query("")

# Load CSV data
df = pd.read_csv("aac_shelter_outcomes.csv")

def get_rescue_type_id(breed, age_weeks):
    for rescue_type, rules in rescue_rules.items():
        if breed in rules['breed'] and age_weeks is not None and age_weeks <= rules['max_age_weeks']:
            return {'water': 1, 'mountain': 2, 'disaster': 3}.get(rescue_type)
    return None

for _, row in df.iterrows():
    try:
        breed = row.get("breed")
        age = row.get("age_upon_outcome_in_weeks")
        age = int(age) if pd.notna(age) else None
        rescue_type_id = get_rescue_type_id(breed, age)

        location_name = row.get("location", "Austin Animal Center")
        location_id = shelter_locations.get(location_name, 1)

        dog_data = {
            "name": row.get("name"),
            "breed": breed,
            "age_upon_outcome_weeks": age,
            "sex_upon_outcome": row.get("sex_upon_outcome"),
            "outcome_type": row.get("outcome_type"),
            "outcome_subtype": row.get("outcome_subtype"),
            "rescue_type_id": rescue_type_id,
            "location_id": location_id,
            "animal_type": row.get("animal_type"),
            "color": row.get("color"),
            "date_of_birth": row.get("date_of_birth"),
            "datetime": row.get("datetime"),
            "monthyear": row.get("monthyear"),
            "location_lat": row.get("location_lat"),
            "location_long": row.get("location_long")
        }

        db.create(dog_data)

    except Exception as e:
        print(f"Error inserting row: {e}")
