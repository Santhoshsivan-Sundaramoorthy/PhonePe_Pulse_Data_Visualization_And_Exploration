import os
import shutil
import git
import data_transform

# Get the current directory by using '.'
current_directory = '.'

# Define the path where you want to store the cloned data
clone_folder = 'pulse_repository'

# Clone the GitHub repository
repo_url = 'https://github.com/phonepe/pulse.git'

if clone_folder in os.listdir(current_directory):
    shutil.rmtree(clone_folder)

git.Repo.clone_from(repo_url, clone_folder)
