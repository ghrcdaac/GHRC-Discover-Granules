import shutil

import pip

pip.main(['install', '--target', './package', '-r', 'requirements.txt'])
[shutil.copy(ele, './package/') for ele in ['./task/granule.py', './task/lambda_function.py', './task/main.py']]
shutil.make_archive('./task/dist/package', 'zip', './package')
shutil.rmtree('./package')
