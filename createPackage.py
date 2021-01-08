import shutil

import pip

pip.main(['install', '--target', './package', '-r', 'requirements.txt'])
shutil.copy('./task/granule.py', './package/')
shutil.copy('./task/lambda_function.py', './package/')
shutil.copy('./task/main.py', './package/')
shutil.make_archive('package', 'zip', './package')
shutil.rmtree('./package')
