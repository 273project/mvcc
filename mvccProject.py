import pickle
import threading
import time
import sqlite3
import getpass
from sqlite3 import Error
from collections import defaultdict
from hashlib import sha256


class Database:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, mvccdb_file):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Database, cls).__new__(cls)
                cls._instance.connection = sqlite3.connect(mvccdb_file)
                cls._instance.init_db()
            return cls._instance

    def init_db(self):
        cursor = self.connection.cursor()
        users_table = """CREATE TABLE IF NOT EXISTS users (
                            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            username TEXT UNIQUE NOT NULL,
                            password_hash TEXT NOT NULL
                        )"""
        data_table = """CREATE TABLE IF NOT EXISTS data (
                            data_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER NOT NULL,
                            content BLOB,
                            version INTEGER ,
                            created_at TEXT ,
                            modified_at TEXT,
                            FOREIGN KEY (user_id) REFERENCES users (user_id)
                        )"""
        index_users = """CREATE INDEX IF NOT EXISTS idx_username ON users(username)"""
        index_data = """CREATE INDEX IF NOT EXISTS idx_user_id ON data(user_id)"""

        cursor.execute(users_table)
        cursor.execute(data_table)
        cursor.execute(index_users)
        cursor.execute(index_data)

        self.connection.commit()


    def close(self):
        self.connection.close()


class SimpleDatabase:
    def __init__(self):
        self.db = Database('mvcc_database.db')
        self.mvcc_system = MVCCSystem()

    def hash_password(self, password):
        return sha256(password.encode()).hexdigest()

    def signup(self, username, password):
        hashed_password = self.hash_password(password)
        cursor = self.db.connection.cursor()
        try:
            cursor.execute(
                "INSERT INTO users(username, password_hash) VALUES (?, ?)", (username, hashed_password))
            self.db.connection.commit()
        except sqlite3.IntegrityError:
            raise Exception("Username already exists")

    def login(self, username, password):
        cursor = self.db.connection.cursor()
        cursor.execute(
            "SELECT password_hash FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        if result and self.hash_password(password) == result[0]:
            return True
        return False

    
    
    def create_data(self, username, data, transaction_id):
        user_id = self.get_user_id(username)
        created_at = time.strftime('%Y-%m-%d %H:%M:%S')
        version = 1
        cursor = self.db.connection.cursor()
        cursor.execute("INSERT INTO data (user_id, content, version, created_at) VALUES (?, ?, ?, ?)",
                   (user_id, data, version, created_at))
        self.db.connection.commit()

    
        object_id = cursor.lastrowid
        self.mvcc_system.write_object(object_id, version, data, transaction_id)
        return object_id
    
    def get_user_data_ids(self, username):
        user_id = self.get_user_id(username)
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT data_id FROM data")
        data_ids = cursor.fetchall()
        return [data_id[0] for data_id in data_ids]

    def read_data(self, object_id, transaction_id):
        return self.mvcc_system.read_object(object_id, transaction_id)

    def update_data(self, object_id, new_data, transaction_id,username):
        
        data_id = int(object_id)  
        user_id = self.get_user_id(username)

    
        current_version = self.mvcc_system.get_current_version(data_id)

    
        new_version = current_version + 1

    
        modified_at = time.strftime('%Y-%m-%d %H:%M:%S')

    
        cursor = self.db.connection.cursor()
        cursor.execute("UPDATE data SET content = ?, version = ?, modified_at = ? WHERE data_id = ? AND user_id = ?",
                   (new_data, new_version, modified_at, data_id, user_id))
        self.db.connection.commit()

    
        self.mvcc_system.write_object(data_id, new_version, new_data, transaction_id)

    def delete_data(self, object_id, transaction_id,username):
        user_id = self.get_user_id(username)

   
        current_version = self.mvcc_system.get_current_version(object_id)

    
        new_version = current_version + 1

    
        modified_at = time.strftime('%Y-%m-%d %H:%M:%S')

    
        cursor = self.db.connection.cursor()
        cursor.execute("UPDATE data SET content = ?, version = ?, modified_at = ? WHERE data_id = ? AND user_id = ?",
                   (None, new_version, modified_at, object_id, user_id))
        self.db.connection.commit()

    
        self.mvcc_system.write_object(object_id, new_version, None, transaction_id)
        
    def get_user_id(self, username):
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT user_id FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise Exception("User not found")


    def close(self):
        self.db.close()


class MVCCSystem:
    def __init__(self):
        
        self.storage = defaultdict(list)
        self.lock = threading.RLock()
        self.db = Database('mvcc_database2.db')
        self.transactions = {}

    def start_transaction(self):
        with self.lock:
            transaction_id = time.time()
        
            self.transactions[transaction_id] = (transaction_id, 'active', {})
            return transaction_id

    def commit_transaction(self, transaction_id):
        with self.lock:
            if transaction_id not in self.transactions:
                raise Exception("Transaction not found")

            start_time, status, changes  = self.transactions[transaction_id]
            if status != 'active':
                raise Exception(
                    "Transaction is not active or already committed/aborted")

            
            if not self.check_conflicts(transaction_id):
                self.abort_transaction(transaction_id)
                raise Exception("Conflict detected, transaction aborted")

           
            self.finalize_changes(transaction_id)
            self.transactions[transaction_id] = (start_time, 'committed')

    def check_conflicts(self, transaction_id):
        with self.lock:
            if transaction_id not in self.transactions:
                raise Exception("Transaction not found")

        start_time, status, _ = self.transactions[transaction_id]  
        for object_id, versions in self.storage.items():
            for version, _, writer_transaction_id in versions:
                if writer_transaction_id == transaction_id:
                    
                    for v, _, other_transaction_id in versions:
                        if other_transaction_id != transaction_id and v > start_time and self.transactions[other_transaction_id][1] == 'committed':
                            return False
        return True

    def finalize_changes(self, transaction_id):
        
        for object_id, versions in self.storage.items():
            self.storage[object_id] = [v for v in versions if v[2] ==
                                       transaction_id or not self.is_version_outdated(v[0])]

    def is_version_outdated(self, version):
        
        return all(version < tx[0] or tx[1] == 'committed' for tx in self.transactions.values())

    def abort_transaction(self, transaction_id):
        
        with self.lock:
            if transaction_id in self.transactions:
                del self.transactions[transaction_id]
                
                
    def get_current_version(self, object_id):
        with self.lock:
            versions = self.storage.get(object_id, [])
        if versions:
            return versions[-1][0]  
        else:
            
            cursor = self.db.connection.cursor()
            cursor.execute("SELECT version FROM data WHERE data_id = ?", (object_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise Exception("Object not found")


    def read_object(self, object_id, transaction_id,ignore_transaction_status=False):
        with self.lock:  
            if transaction_id not in self.transactions and not ignore_transaction_status:
                raise Exception("Transaction not found")

        if not ignore_transaction_status:
            start_time, status, _ = self.transactions[transaction_id]
            if status != 'active':
                raise Exception("Transaction is not active")

        versions = self.storage.get(object_id, [])
        for version, serialized_obj, _ in reversed(versions):
            if version <= start_time:
                return self.deserialize(serialized_obj)

       
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT content FROM data WHERE data_id = ?", (object_id))
        result = cursor.fetchone()
        if result:
            serialized_obj = result[0]
            return self.deserialize(serialized_obj)

        raise Exception("Object not found")

    def write_object(self, object_id, new_version, new_data, transaction_id):
        with self.lock:
            if transaction_id not in self.transactions:
                raise Exception("Transaction not found")

        _, status, changes = self.transactions[transaction_id]
        if status != 'active':
            raise Exception("Transaction is not active")

        
        for _, _, other_transaction_id in self.storage[object_id]:
            if other_transaction_id != transaction_id and self.transactions[other_transaction_id][1] == 'active':
                raise Exception("Write-write conflict detected with another active transaction")

        serialized_obj = self.serialize(new_data)
        modified_at = time.strftime('%Y-%m-%d %H:%M:%S')



        cursor = self.db.connection.cursor()
        cursor.execute("UPDATE data SET content = ?, version = ?, modified_at = ? WHERE data_id = ?",
                    (serialized_obj, new_version, modified_at, object_id))
        self.db.connection.commit()

        self.storage[object_id].append((new_version, serialized_obj, transaction_id))

    def rollback_transaction(self, transaction_id):
        with self.lock:
            if transaction_id not in self.transactions:
                raise Exception("Transaction not found")

            _, status, changes = self.transactions[transaction_id]
            if status != 'active':
                raise Exception("Transaction is not active")

            
            for object_id, original_data in changes.items():
                if original_data is not None:
                    # Revert to original data
                    self.write_object(object_id, original_data['version'], original_data['data'], transaction_id)

           
            self.transactions[transaction_id] = (self.transactions[transaction_id][0], 'aborted')

    def serialize(self, obj):
        return pickle.dumps(obj)

    def deserialize(self, serialized_obj):
        return pickle.loads(serialized_obj)

    def cleanup_versions(self):
        with self.lock:
            active_start_times = {tx[0] for tx in self.transactions.values() if tx[1] == 'active'}
            for object_id, versions in list(self.storage.items()):
                
                self.storage[object_id] = [v for v in versions if any(v[0] > start_time for start_time in active_start_times)]

         
                if not self.storage[object_id]:
                    del self.storage[object_id]

    def start_periodic_cleanup(self, interval=60):
        """Starts a background thread that performs periodic cleanup."""
        def cleanup_task():
            while True:
                self.cleanup_versions()
                time.sleep(interval)

        cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
        cleanup_thread.start()


class SimpleDatabaseCLI:
    def __init__(self):
        self.db = SimpleDatabase()
        self.current_user = None
        self.running = True

    def run(self):
        try:
            while self.running:
                if not self.current_user:
                    self.show_main_menu()
                else:
                    self.show_user_menu()
        finally:
            self.db.close()

    def show_main_menu(self):
        print("\n1. Signup\n2. Login\n3. Exit")
        choice = input("Enter choice: ")

        if choice == '1':
            self.signup()
        elif choice == '2':
            self.login()
        elif choice == '3':
            self.running = False

    def signup(self):
        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ") 
        try:
            self.db.signup(username, password)
            print("Signup successful")
        except Exception as e:
            print(str(e))

    def login(self):
        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        if self.db.login(username, password):
            self.current_user = username
            print("Login successful")
        else:
            print("Invalid username or password")
    
    def show_user_data_ids(self):
        
        try:
            data_ids = self.db.get_user_data_ids(self.current_user)
            if data_ids:
                print("\nYour Data IDs are:")
                for data_id in data_ids:
                    print(data_id)
            else:
                print("\nYou have no data.")
        except Exception as e:
            print(f"Error fetching data IDs: {e}")

    def show_user_menu(self):
        print("\n1. Create Data\n2. Read Data\n3. Update Data\n4. Delete Data\n5. Show all Data IDs\n6. Logout")
        choice = input("Enter choice: ")

        if choice == '1':
            self.create_data()
        elif choice == '2':
            self.read_data()
        elif choice == '3':
            self.update_data()
        elif choice == '4':
            self.delete_data()
        elif choice == '5':
            self.show_user_data_ids()
        elif choice == '6':
            self.current_user = None

    def create_data(self):
        data = input("Enter Content: ")
        transaction_id = self.db.mvcc_system.start_transaction()
        object_id = self.db.create_data(
            self.current_user, data, transaction_id)
        self.db.mvcc_system.commit_transaction(transaction_id)
        print(f"Data created with ID: {object_id}")

    def read_data(self):
        object_id = input("Enter data ID: ")
        transaction_id = self.db.mvcc_system.start_transaction()
        try:
            data = self.db.read_data(object_id, transaction_id)
            print("Data:", data)
        except Exception as e:
            print(str(e))
        finally:
            self.db.mvcc_system.commit_transaction(transaction_id)

    def update_data(self):
        object_id = input("Enter data ID: ")
        new_data = input("Enter new data: ")
        transaction_id = self.db.mvcc_system.start_transaction()
        try:
            self.db.update_data(object_id, new_data, transaction_id,self.current_user)
            print("Data updated")
        except Exception as e:
            print(str(e))
        finally:
            self.db.mvcc_system.commit_transaction(transaction_id)

    def delete_data(self):
        object_id = input("Enter data ID: ")
        transaction_id = self.db.mvcc_system.start_transaction()
        try:
            self.db.delete_data(object_id, transaction_id,self.current_user)
            print("Data deleted")
        except Exception as e:
            print(str(e))
        finally:
            self.db.mvcc_system.commit_transaction(transaction_id)



cli = SimpleDatabaseCLI()
cli.run()
