import setuptools

setuptools.setup(
     name='lastFMProject',
     version='0.1',
     packages=['cdc', 'api_utilities', 'destination'],
     scripts=['cdc/listening_sessions_cdc.py', 'cdc/songs_cdc.py', \
              'api_utilities/songs_batch_api_source.py', 'api_utilities/users_batch_api_source.py',\
              'destination/cloud_datalake.py'] ,
     author="Data Adepts",
     author_email="",
     description="LastFM project",
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: GPL-3.0",
         "Operating System :: Linux base",
     ],
 )