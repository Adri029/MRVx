# Multi-Record Values *

This repository is a library of structures making use of the Multi-Record Values technique for relational databases.

[Multi-Record Values](https://github.com/nuno-faria/mrv)

Create the MRVs*:
- Create a `.yml` that specifies which columns of which tables to model as MRVs. The `example_model.yml` file can be used as a starting point;
- Choose the desired converter file from 'mrvx_structures' or 'specialized_structures';
- Refactor the schema: 'python3 <converter.py> <model.yml>';
