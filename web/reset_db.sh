#find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
#rm db.sqlite3
python manage.py makemigrations
python manage.py migrate
#pytest --create-db

