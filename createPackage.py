import os
import shutil
import pip

pip.main(['install', '--target', './package', '-r', 'requirements-lambda.txt'])
os.makedirs('./package/task')
for ele in ['./task/lambda_function.py', './task/main.py', './task/dgm.py', './task/__init__.py']:
    shutil.copy(ele, './package/task')
shutil.make_archive('./package', 'zip', './package')
shutil.rmtree('./package')
