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
            conn.execute("PRAGMA journal_mode=WAL;")
            cursor = conn.cursor()

            # Create table with unique constraints on both username and email
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    email TEXT UNIQUE,
                    avatar TEXT,
                    role TEXT DEFAULT 'user',
                    created_at REAL,
                    last_login REAL
                )
            ''')

            # Create user_follows table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_follows (
                    follower_id INTEGER,
                    followed_id INTEGER,
                    created_at REAL,
                    PRIMARY KEY (follower_id, followed_id)
                )
            ''')

            # MIGRATION: Add case-insensitive UNIQUE constraint to username if it's missing
            try:
                # This is an idempotent way to ensure the index exists.
                # It will enforce case-insensitive uniqueness.
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_nocase ON users (username COLLATE NOCASE);")
                print("MIGRATION: Ensured username column is unique and case-insensitive.")
            except sqlite3.OperationalError as e:
                if "UNIQUE constraint failed" in str(e):
                    print("\n" + "="*60)
                    print("MIGRATION FAILED: Could not make username unique.")
                    print("REASON: Your database contains duplicate usernames (e.g., 'user' and 'User').")
                    print("ACTION: Please resolve duplicates manually before restarting the application.")
                    print("="*60 + "\n")
                    # Exit because the app cannot run correctly with this state.
                    exit(1)
                else:
                    pass # Another error, maybe index already exists. Safe to ignore.

            conn.commit()

            # Migration: Ensure other profile columns exist for existing databases
            new_cols = [("avatar", "TEXT"), ("bio", "TEXT"), ("website", "TEXT"), ("instagram", "TEXT"), ("twitter", "TEXT"), ("contact_email", "TEXT")]
            for col, dtype in new_cols:
                try:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {col} {dtype}")
                    conn.commit()
                except sqlite3.OperationalError:
                    pass # Column likely exists

            conn.close()
        except Exception as e:
            print(f"Error initializing user database: {e}")

    def create_user(self, username, password, email=None, role='user', avatar=None):
        """Creates a new user with a hashed password."""
        if not username or not password or not email:
            return {'success': False, 'message': 'MISSING_REQUIRED_FIELDS'}

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
            return {'success': True, 'message': 'USER_CREATED', 'user_id': user_id}
        except sqlite3.IntegrityError as e:
            if 'email' in str(e).lower():
                return {'success': False, 'message': 'EMAIL_ALREADY_REGISTERED'}
            elif 'username' in str(e).lower():
                return {'success': False, 'message': 'USERNAME_ALREADY_TAKEN'}
            return {'success': False, 'message': 'USER_CREATION_FAILED'} # Generic integrity error
        except Exception as e:
            return {'success': False, 'message': 'USER_CREATION_FAILED'}
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
                    'message': 'AUTH_SUCCESS', 
                    'user_id': user[0], 
                    'role': user[2], 
                    'username': user[3],
                    'avatar': avatar_val
                }
            
            return {'success': False, 'message': 'INVALID_CREDENTIALS'}
        except Exception as e:
            return {'success': False, 'message': 'AUTH_ERROR'}
        finally:
            if conn:
                conn.close()

    def get_user_by_username(self, username):
        """Retrieves a user's public data by their username."""
        conn = None
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Use COLLATE NOCASE for case-insensitive matching, which is good for URLs.
            cursor.execute('SELECT * FROM users WHERE username = ? COLLATE NOCASE', (username,))
            user = cursor.fetchone()
            if user:
                return dict(user)
            return None
        except Exception as e:
            print(f"Error getting user by username: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_user_by_id(self, user_id):
        """Retrieves a user's public data by their ID."""
        conn = None
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            user = cursor.fetchone()
            if user:
                return dict(user)
            return None
        except Exception as e:
            print(f"Error getting user by id: {e}")
            return None
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

    def toggle_follow(self, follower_id, target_id):
        """Toggles follow status. Returns dictionary with success, action ('followed'/'unfollowed'), and message."""
        if int(follower_id) == int(target_id):
            return {'success': False, 'message': 'Cannot follow yourself'}

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM user_follows WHERE follower_id = ? AND followed_id = ?", (follower_id, target_id))
            if cursor.fetchone():
                cursor.execute("DELETE FROM user_follows WHERE follower_id = ? AND followed_id = ?", (follower_id, target_id))
                action = 'unfollowed'
            else:
                cursor.execute("INSERT INTO user_follows (follower_id, followed_id, created_at) VALUES (?, ?, ?)", (follower_id, target_id, time.time()))
                action = 'followed'
            conn.commit()
            return {'success': True, 'action': action}
        except Exception as e:
            return {'success': False, 'message': str(e)}
        finally:
            conn.close()

    def get_follow_stats(self, user_id):
        """Returns follower and following counts for a user."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM user_follows WHERE followed_id = ?", (user_id,))
            followers = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM user_follows WHERE follower_id = ?", (user_id,))
            following = cursor.fetchone()[0]
            return {'followers': followers, 'following': following}
        except Exception:
            return {'followers': 0, 'following': 0}
        finally:
            conn.close()

    def is_following(self, follower_id, target_id):
        """Checks if follower_id is following target_id."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM user_follows WHERE follower_id = ? AND followed_id = ?", (follower_id, target_id))
            return cursor.fetchone() is not None
        except Exception:
            return False
        finally:
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