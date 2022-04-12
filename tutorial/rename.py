import os

for directory in os.listdir('out'):
    filename = os.listdir(f'out/{directory}')
    if len(filename) > 0:
        filename = filename[0]
        if '.json' not in filename:
            print(directory, filename)
            os.rename(f'out/{directory}/{filename}', f'out/{directory}/{filename}.json')
