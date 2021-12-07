# Requirements
- pyenv for python version control 
- pipenv for python package control
- R and R studio

# Installation
- install pyenv from following link https://github.com/pyenv/pyenv
- install pipenv from following link https://github.com/pypa/pipenv
- Install R from https://linuxize.com/post/how-to-install-r-on-ubuntu-20-04/
- install Rstudio https://www.rstudio.com/products/rstudio/download/
- Install IRkernel to run R script in Jupyter notebook/lab

# Note for elyra
- elyra will raise error when python version is 3.9.x with pipenv
- tested with v3.7.12 with poetry [successful]  

# python version
- please use 3.7.12 [i.e align with Kaggle notebook]

# Dependencies
- add in the requirements.txt 
- or pipenv automatically install the required packages

# Start project
- clone the repo and run pipenv install
- If dependencies error will occure, please install pre
- this method will be easy for Unix family

# For first time run
- Jupyter lab need to rebuild some packages such as elyra


# Note
- Notebook for prototyping and check data information
- Notebook may be either R or Python 
- Install R kernel with requried packages
- Note visulization is not for DASH or streamlit [we will use viola or bokeh]
- data will be written as batch mode to save in DB
- data dashboard will be apache supreset

