# main.py
from cli import MyDBCLI
import atexit
from db_manager import get_database_filename, load_or_create_database


if __name__ == "__main__":
    filename = get_database_filename()
    mydb_cli = MyDBCLI()
    filename = load_or_create_database(filename, mydb_cli)
    mydb_cli.set_directory(filename)

    def save_on_exit():
        mydb_cli.mydb.save_metadata(filename)

    atexit.register(save_on_exit)

    mydb_cli.run()
