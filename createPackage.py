import os
import shutil
import pip
import re

temp_dir = './package'
try:
    pip.main(['install', '--target', './package', '-r', 'requirements.txt'])
    shutil.make_archive('dependencies', 'zip', temp_dir)
    # Remove boto3 and botocore from the package as they are provided in the lambda environment. Simply omitting them
    # from the requirements.txt file is not sufficient as other dependencies can include them.
    for i in os.listdir('./package'):
        match = re.search(r'boto(3|(core)).*', i)
        if match:
            shutil.rmtree(f'./package/{match[0]}')
            print(f'Removed {match[0]} from package.')
    shutil.make_archive('dependencies_lambda', 'zip', temp_dir)

    sub_dir = f'{temp_dir}/task'
    os.makedirs(sub_dir)
    for ele in ['./task/lambda_function.py', './task/main.py', './task/dgm.py', './task/__init__.py']:
        shutil.copy(ele, sub_dir)
    shutil.make_archive(temp_dir, 'zip', temp_dir)
    shutil.make_archive('source', 'zip', sub_dir)
except Exception as e:
    print(f'There was an exception: {e}')
print('Cleaning up packaging directory.')
shutil.rmtree('./package')

