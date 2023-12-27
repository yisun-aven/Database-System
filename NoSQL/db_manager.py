# db_manager.py
import os


def get_database_filename():
    while True:
        filename = input("Enter the database: ")
        if not filename:
            print("Please provide a valid database.")
        else:
            return filename


def load_or_create_database(directory, mydb_cli):
    # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory}"
    current_directory = os.getcwd()
    root_path = os.path.join(current_directory, "myDB")
    print("Root Path", root_path)

    # check if myDB exists or not
    if not os.path.exists(root_path):
        os.mkdir(root_path)
        os.mkdir(f'{root_path}/Database')

    # Establish a full_path to database
    full_path = os.path.join(current_directory, "myDB", "Database", directory)

    if not os.path.exists(full_path):
        # The database file doesn't exist; offer to create a new one
        create_new_db = input(f"The database file '{directory}' doesn't exist. Create a new one? (yes/no): ").strip().lower()
        if create_new_db == 'yes':
            os.mkdir(full_path)  # create directories db1, db2...
            os.mkdir(f'{full_path}/tables')  # create directories tables that store all the table files
            mydb_cli.mydb.save_metadata(directory)
            return directory
        else:
            newFilename = get_database_filename()
            return load_or_create_database(newFilename, mydb_cli)
    else:
        mydb_cli.mydb.load_metadata(directory)
        return directory
