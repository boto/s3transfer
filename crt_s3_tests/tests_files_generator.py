import os

folder_name = "small_files"

if folder_name not in os.listdir():
    os.mkdir(folder_name)

os.chdir(folder_name)

files_num = 1

file_length = 10000000000

for i in range(0, files_num):
    file_name = str(i) + "_10GB.txt"
    file = open(file_name, "w")
    content = str(i)
    while len(content) < file_length:
        content = content+content
    file.write(content[:file_length])
    file.close()
