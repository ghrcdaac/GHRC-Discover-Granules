import shutil
import pip

pip.main(['install', '--target', './package', '-r', 'requirements-lambda.txt'])
[shutil.copy(ele, './package/') for ele in ['./task/lambda_function.py', './task/main.py']]
shutil.make_archive('./package', 'zip', './package')
shutil.rmtree('./package')
