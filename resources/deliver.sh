projDir="/c/Users/ET80860/Documents/workspaces/pyBackup"
distDir="~/pyBackup"


for f in backup.py webhdfs.py test/testWebHdfs; do
    scp -r "$projDir/$f" "ET20795@sriopmgta0101.recette.local:$distDir/$f"
done


#launch test

cd ~/pyBackup
export PYTHONPATH="$(pwd)"
python ./test/testWebHdfs.py
