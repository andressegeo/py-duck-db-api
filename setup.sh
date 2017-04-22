VENV_FOLDER = "venv"
if [ ! -d "$VENV_FOLDER" ]; then
    rm -rf venv/
fi
virtualenv -p python2 venv/
source venv/bin/activate
pip2 install -r requirements.txt
