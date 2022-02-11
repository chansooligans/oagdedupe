# oagdedupe  


Goals: Dedupe package by RAD  

## Case Team  
Division(s): Other  
Bureau(s):  
Assistant Attorney General(s):  
Research Staff(s): RAD    

## Table of Contents

1. [Documentation](#documentation)  
	a. [High-level](#high-level)  
	b. [Key events](#key-events)  
	c. [Data](#data)  
	d. [Work product](#work-product)  
	e. [Communications with case team](#communications-with-case-team)  
	f. [Dependencies](#dependencies)  
1. [Repository structure](#repository-structure)
2. [READMEs](#readmes)
3. [Results](#results)
4. [Next Steps](#next-steps)

## Documentation

[can use templates below, or link to documentation]

### High-level

[high-level narrative description of the case and project]

### Key events

[can use the separate markdown file at [references/keyevents.md](references/keyevents.md)]

[document the key events/decisions/forks on project direction]

[repeat for each event]

#### Key event 1

|question|answer|
|-|-|
|what happened?||
|when did it happen?||

### Data

[can use the separate markdown file at [references/data.md](references/data.md)]

[repeat for each original or processed data set]

#### [data set 1 name]

|question|answer|
|-|-|
|what is it?|[general description]|
|where is it?|[filepath(s) or redshift table name(s)]|
|is it original or processed?|[original, processed or a description if not so easy to characterize]|
|where did it come from?|[url(s), link to email(s), reference to the data processed from]|
|when was it pulled/processed?|[need not answer if date is in the file/table name or description or the data set was pulled/processed when the script was committed]|
|is the source data static?|[is the source data set expected to change over time? is this pulled/processed data set a snapshot of the source data set?]|
|how was it pulled/processed?|[description and/or link to script(s)]|
|is it sensitive?||
|what does each row represent?||

<details>

##### Data dictionary

[can replace table below with a link to a data dictionary]

|question|answer|
|-|-|
|which fields uniquely identify the rows?||
|are there null values?||
|are there duplicates?||
|are there outliers?||
|what is the time period covered?||
|what is the geography covered?||
|how many rows?||

|field|description|
|-|-|
|[field 1 name]|[description of field 1]|
|[field 2 name]|[description of field 2]|

</details>

### Work product

[can use the separate markdown file at [references/work_product.md](references/work_product.md)]

<details>

[use this section to document work products sent to the case team (e.g. word/excel documents, notebooks, dashboards, charts, emails with analysis results)]  

#### [work product 1 name]


|question|answer|
|-|-|
|what is it?|[general description]|
|which asks does this address?|[problem definition, main question, the issue that is resolved]|
|where is it?|[filepath or url]|
|how was it created?|[description and/or link to script]|
|when was it created?||
|when was it sent to the case team?||
|how was it sent to the case team?|[description or link to email]|

</details>

### Communications with case team

[can use the separate markdown file at [references/communications.md](references/communications.md)]

<details>

[use this section to document communications with the case team where decisions were made (e.g. meetings, emails)] 

#### [communication 1 name]

|question|answer|
|-|-|
|what was it?|[general description]|
|when was it?||
|where is it?|[link to email]|
|what decisions were made?|[general description, can list more specific asks in the section below]|

##### [ask 1 coming from communication 1]

|question|answer|
|-|-|
|what is the ask?|[general description]|
|when is it needed by?||
|what is the status?|[not yet started, currently working on, or completed]|
|when was it completed?||
|which work products address this ask?||

</details>

### Dependencies

[can use the separate markdown file at [references/dependencies.md](references/dependencies.md)]

[document any development environment dependencies on top of dumpling required to run the code in this project (e.g. installing a python package)]

#### Dependency 1

|question|answer|
|-|-|
|what is it?||
|how did you install it?||
|when did you install it?||
|why do you need it?||


## Repository Structure

	.
	|-- README.md
	|-- scripts					# Scripts				
	|-- notebooks				# Jupyter notebooks. Naming convention is a number (for ordering),
								the creator's initials, and a short `-` delimited description, e.g.
								`1.0-jqp-initial-data-exploration`
	|-- references				# Data dictionaries, manuals, and all other explanatory materials.
	|-- methodology				# Outlines steps to upload, wrangle, clean, analyze, visualize
	|-- reports  				# Generated analysis as HTML, PDF, LaTeX, etc.
		|-- figures			# Generated graphics and figures to be used in reporting
	|-- requirements.txt 			# The requirements file for reproducing the analysis environment, e.g.
								generated with `pip freeze > requirements.txt`
	|-- setup.py 				# Make this project pip installable with `pip install -e`
	|-- oagdedupe 	# Source code for use in this project.
		|-- __init__.py 		# Makes oagdedupe a Python module
		|-- data 			# Scripts to download or generate data
		|-- preprocess			# Scripts for cleaning, wrangling, feature creation, etc...
		|-- analysis 			# Scripts to generate outputs
		|-- visualization 		# Scripts to create exploratory and results oriented visualizations
	|-- fact_check				# Blind peer fact check prior to external distribution
	|-- archive


## READMEs

1. [notebooks/README.md](notebooks/README.md)
2. [references/README.md](references/README.md)
3. [methodology/README.md](methodology/README.md)
4. [src/README.md](src/README.md)
5. [fact_check/README.md](fact_check/README.md)

## Results

[Overview summary (Optional: add date).]

## Next Steps  

[Note any next steps in the case or research.]



## Credits

This package was created with [Cookiecutter](https://github.com/cookiecutter/cookiecutter) and the [cookiecutter-rad](https://github.com/NYAG/cookiecutter-rad) project template.