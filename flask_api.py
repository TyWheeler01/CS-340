from flask import Flask, jsonify, request
from flask_login import LoginManager, UserMixin, login_user, login_required, current_user
from crud import AnimalShelter

app = Flask(__name__)
app.secret_key = 'key'

# Setup Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)

# Mock user database
users = {
    'admin': {'password': 'adminpass', 'role': 'admin'},
    'viewer': {'password': 'viewerpass', 'role': 'viewer'}
}

# User model
class User(UserMixin):
    def __init__(self, username, role):
        self.id = username
        self.role = role

    def get_role(self):
        return self.role

@login_manager.user_loader
def load_user(user_id):
    user_info = users.get(user_id)
    if user_info:
        return User(user_id, user_info['role'])
    return None

# Login endpoint
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")
    user = users.get(username)

    if user and user['password'] == password:
        login_user(User(username, user['role']))
        return jsonify({"message": f"Welcome, {username}!"})
    return jsonify({"error": "Invalid credentials"}), 401

# Secure GET endpoint for all dogs
@app.route('/dogs')
@login_required
def get_dogs():
    if not current_user.is_authenticated:
        return jsonify({"error": "Unauthorized"}), 401

    db = AnimalShelter()
    results = db.read()
    columns = [
        "id", "name", "breed", "age_upon_outcome_weeks", "sex_upon_outcome",
        "animal_type", "color", "date_of_birth", "datetime", "monthyear",
        "outcome_type", "outcome_subtype", "location_lat", "location_long"
    ]
    dogs = [dict(zip(columns, row)) for row in results]
    return jsonify(dogs)

# Admin only DELETE endpoint
@app.route('/dogs/<int:dog_id>', methods=['DELETE'])
@login_required
def delete_dog(dog_id):
    if not current_user.is_authenticated:
        return jsonify({"error": "Unauthorized"}), 401

    if current_user.get_role() != 'admin':
        return jsonify({"error": "Forbidden: Admins only"}), 403

    db = AnimalShelter()
    deleted = db.delete(dog_id)

    if deleted:
        return jsonify({"message": f"Dog with ID {dog_id} deleted."}), 200
    else:
        return jsonify({"error": "Dog not found or already deleted"}), 404

if __name__ == '__main__':
    app.run(debug=True)
