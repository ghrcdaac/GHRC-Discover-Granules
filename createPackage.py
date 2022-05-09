import os
import shutil
import pip

pip.main(['install', '--target', './package', '-r', 'requirements.txt'])
os.makedirs('./package/task')
dir_path = os.getcwd()
print(f'Searching: {dir_path}')
# for ele in ['./task/lambda_function.py', './task/main.py', './task/dgm.py', './task/__init__.py',
#             './task/discover_granules_base.py', './task/discover_granules_http.py', './task/discover_granules_s3.py',
#             './task/helpers.py']:
for ele in os.listdir(f'{dir_path}/task'):
    if '.py' in ele:
        print(f'COpying: {ele}')
        shutil.copy(f'./task/{ele}', './package/task')
shutil.make_archive('./package', 'zip', './package')
shutil.rmtree('./package')
