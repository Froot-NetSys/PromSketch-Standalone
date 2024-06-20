kill -9 $(ps aux | grep  -i '[p]ython Exportmanager.py' | awk '{print $2}')
