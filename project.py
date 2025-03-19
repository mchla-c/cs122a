import mysql.connector
import os

def import_data(folder_name):
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="test",
            passwd="password",
            database="cs122a",
            allow_local_infile=True
        )
        cursor = db.cursor()
        cursor.execute("SET foreign_key_checks = 0;")
        # Drop existing tables
        tables_to_drop = ["users", "producers", "viewers", "releases", "movies", "series", "videos", "sessions", "reviews"]
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        # print("Tables dropped successfully.")

        # Create new tables 
        create_tables(cursor)
        # print("Tables created successfully.")
        cursor.execute("SET foreign_key_checks = 1;")


        # Load data from CSV files
        for file in os.listdir(folder_name):
            if file.endswith('.csv'):
                table_name = file.split('.')[0]  # Assuming the CSV file name matches the table name
                csv_file_path = os.path.join(folder_name, file)
                cursor.execute(f"""
                    LOAD DATA LOCAL INFILE '{csv_file_path}'
                    INTO TABLE {table_name}
                    FIELDS TERMINATED BY ','
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 ROWS;
                """)
                # print(f"Data loaded into {table_name}.")

        # Commit and close
        db.commit()
        cursor.close()
        db.close()

        return True  # Success

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False  # Failure

def create_tables(cursor):
    """Create new tables based on DDL (ensure you have the correct DDL statements)."""
    ddl_statements = [
        """
        CREATE TABLE users (
            user_id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100)
        );
        """,
        """
        CREATE TABLE producers (
            producer_id INT PRIMARY KEY,
            name VARCHAR(100)
        );
        """,
        """
        CREATE TABLE viewers (
            viewer_id INT PRIMARY KEY,
            name VARCHAR(100)
        );
        """,
        """
        CREATE TABLE movies (
            movie_id INT PRIMARY KEY,
            title VARCHAR(100),
            release_year INT
        );
        """,
        """
        CREATE TABLE series (
            series_id INT PRIMARY KEY,
            title VARCHAR(100),
            release_year INT
        );
        """,
        """
        CREATE TABLE videos (
            video_id INT PRIMARY KEY,
            video_title VARCHAR(100)
        );
        """,
        """
        CREATE TABLE sessions (
            session_id INT PRIMARY KEY,
            user_id INT,
            video_id INT,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        );
        """,
        """
        CREATE TABLE reviews (
            review_id INT PRIMARY KEY,
            user_id INT,
            movie_id INT,
            rating INT,
            comment TEXT,
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        );
        """,
        """
        CREATE TABLE releases (
            release_id INT PRIMARY KEY,
            movie_id INT,
            release_date DATE,
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        );
        """
    ]
    
    for ddl in ddl_statements:
        cursor.execute(ddl)

# trigger the import process
if __name__ == "__main__":
    folder_name = "test_data"  # Example folder
    result = import_data(folder_name)
    print(result)  # This will output either True or False

