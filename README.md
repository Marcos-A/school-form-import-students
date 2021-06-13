# school-form-import-students
Populates the school-form database with the students and their enrolled subjects information. This step precedes the submission of the form to the students to be answered.

Designed as a helper to set up: [https://github.com/Marcos-A/teaching-stats](https://github.com/Marcos-A/teaching-stats)

---

### Requirements:
1. Install:

```
sudo apt-get install python3	
sudo apt install python3-pip
pip3 install psycopg2-binary
```

2. Set up your "database.ini" file and place it in the project's root folder. Note the schema should be set to "master" as seen here:

```
[postgresql]
host=YOUR-HOST
database=YOUR-DATABASE
user=YOUR-USER
password=YOUR-PASSWORD
port=YOUR-PORT
options=-c search_path=dbo,master
```

3. Replace the content of the "cf_students.csv" and "eso-batx_students.csv" files located at the "input" folder with your own students data.

---

### How to run
From within the project's root folder, run:

`python3 insert_students.py`
