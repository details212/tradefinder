:loop
python polygon_download.py --days 14 --workers 20 --normalized
python calculate_studies.py
echo Sleeping for 25 minutes...
timeout /t 1500 /nobreak
goto loop