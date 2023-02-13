import subprocess, os

# Reset all the Cloud Run services to min-instances of 0 and stop Cloud SQL
print('\nResetting services ..')
path = os.path.dirname(os.path.realpath(__file__)) 
result = subprocess.run(['sh', path + os.sep + 'reset_services.sh'], stdout=subprocess.PIPE)
result.stdout
print('Services stopped\n')
