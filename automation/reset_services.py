import subprocess
result = subprocess.run(['sh', 'reset_services.sh'], stdout=subprocess.PIPE)
result.stdout