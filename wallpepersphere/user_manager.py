import sqlite3
import time
import os
from werkzeug.security import generate_password_hash, check_password_hash

class UserManager:
    def __init__(self, db_name):
        """Initialize the UserManager with a database file."""
        self.db_name = db_name
        self.init_db()

    def get_connection(self):
        """Creates a database connection."""
        return sqlite3.connect(self.db_name)

    def init_db(self):
        """Initializes the users database table if it doesn't exist."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create table with the new schema if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    email TEXT UNIQUE,
                    avatar TEXT,
                    role TEXT DEFAULT 'user',
                    created_at REAL,
                    last_login REAL
                )
            ''')

            # Check if migration is needed by looking for an old unique index on username
            is_migration_needed = False
            try:
                cursor.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='users' AND sql LIKE '%UNIQUE%username%'")
                if cursor.fetchone():
                    is_migration_needed = True
            except Exception:
                pass # Table might not exist yet, which is fine.

            if is_migration_needed:
                print("MIGRATION: Old schema detected. Migrating users table to enforce unique emails instead of usernames.")
                try:
                    cursor.execute('BEGIN TRANSACTION;')
                    # The standard SQLite way to drop a constraint: rename, create, copy, drop.
                    cursor.execute('ALTER TABLE users RENAME TO users_old;')
                    cursor.execute('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, password_hash TEXT NOT NULL, email TEXT UNIQUE, avatar TEXT, role TEXT DEFAULT \'user\', created_at REAL, last_login REAL)')
                    cursor.execute('INSERT INTO users (id, username, password_hash, email, avatar, role, created_at, last_login) SELECT id, username, password_hash, email, avatar, role, created_at, last_login FROM users_old')
                    cursor.execute('DROP TABLE users_old;')
                    conn.commit()
                    print("MIGRATION: Success.")
                except Exception as e:
                    conn.rollback()
                    print(f"MIGRATION FAILED: {e}. This may be due to duplicate emails in your existing database. Please resolve manually.")

            conn.commit()
            
            # Migration: Ensure avatar column exists for existing databases
            try:
                conn.execute("ALTER TABLE users ADD COLUMN avatar TEXT")
                conn.commit()
            except sqlite3.OperationalError:
                pass # Column likely exists
            conn.close()
        except Exception as e:
            print(f"Error initializing user database: {e}")

    def create_user(self, username, password, email=None, role='user', avatar=None):
        """Creates a new user with a hashed password."""
        if not username or not password or not email:
            return {'success': False, 'message': 'Username, password, and email are required.'}

        # Hash the password for security
        password_hash = generate_password_hash(password)
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                'INSERT INTO users (username, password_hash, email, role, avatar, created_at) VALUES (?, ?, ?, ?, ?, ?)',
                (username, password_hash, email, role, avatar, time.time())
            )
            conn.commit()
            user_id = cursor.lastrowid
            return {'success': True, 'message': 'User created successfully.', 'user_id': user_id}
        except sqlite3.IntegrityError:
            return {'success': False, 'message': 'Email already registered.'}
        except Exception as e:
            return {'success': False, 'message': f'Error creating user: {str(e)}'}
        finally:
            if conn:
                conn.close()

    def authenticate_user(self, identifier, password):
        """Verifies user credentials (email) and updates last login time."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT id, password_hash, role, username, avatar FROM users WHERE email = ?', (identifier,))
            user = cursor.fetchone()

            if user and check_password_hash(user[1], password):
                # Update last login
                cursor.execute('UPDATE users SET last_login = ? WHERE id = ?', (time.time(), user[0]))
                conn.commit()

                avatar_val = user[4]
                if avatar_val and isinstance(avatar_val, str) and avatar_val.startswith('/static/avatars/'):
                    avatar_val = avatar_val.replace('/static/avatars/', '')

                return {
                    'success': True, 
                    'message': 'Authentication successful.', 
                    'user_id': user[0], 
                    'role': user[2], 
                    'username': user[3],
                    'avatar': avatar_val
                }
            
            return {'success': False, 'message': 'Invalid email or password.'}
        except Exception as e:
            return {'success': False, 'message': f'Authentication error: {str(e)}'}
        finally:
            if conn:
                conn.close()

    def update_avatar(self, username, avatar_filename):
        """Updates the user's avatar."""
        conn = None
        try:
            conn = self.get_connection()
            conn.execute('UPDATE users SET avatar = ? WHERE username = ?', (avatar_filename, username))
            conn.commit()
            return {'success': True}
        except Exception as e:
            return {'success': False, 'message': str(e)}
        finally:
            if conn:
                conn.close()

    def delete_user(self, username):
        """Deletes a user by username."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM users WHERE username = ?', (username,))
            rows_affected = cursor.rowcount
            conn.commit()
            
            if rows_affected > 0:
                return {'success': True, 'message': f'User {username} deleted.'}
            return {'success': False, 'message': 'User not found.'}
        except Exception as e:
            return {'success': False, 'message': str(e)}
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    # Simple CLI for testing the system independently
    manager = UserManager(db_name='local_test_users.db')
    print("=== User Manager System ===")
    
    while True:
        print("\n1. Create User")
        print("2. Login")
        print("3. Delete User")
        print("4. Exit")
        choice = input("Select an option: ")

        if choice == '1':
            print(manager.create_user(input("Username: "), input("Password: "), input("Email: ")))
        elif choice == '2':
            print(manager.authenticate_user(input("Username: "), input("Password: ")))
        elif choice == '3':
            print(manager.delete_user(input("Username to delete: ")))
        elif choice == '4':
            break