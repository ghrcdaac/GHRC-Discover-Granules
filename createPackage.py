import os
import shutil
import pip

print(os.getcwd())
pip.main(['install', '--target', './package', '-r', 'requirements.txt'])
os.makedirs('./package/task')
task_dir = f'{os.getcwd()}/task'
for ele in os.listdir(task_dir):
    if ele.endswith('.py'):
        shutil.copy(f'task/{ele}', './package/task')

shutil.make_archive('./package', 'zip', './package')
print(f'Wrote to {os.getcwd()}')
shutil.rmtree('./package')


